function setCookie(cname, cvalue, exdays) {
    var d = new Date();
    d.setTime(d.getTime() + (exdays * 24 * 60 * 60 * 1000));
    var expires = "expires=" + d.toUTCString();
    document.cookie = cname + "=" + cvalue + ";" + expires + ";path=/";
}
//------
$('#datepicker').daterangepicker({
    startDate: moment().subtract(89, 'days'),
    endDate: moment(),
    opens: 'left',
    ranges: {
        'Last 30 Days': [moment().subtract(29, 'days'), moment()],
        'Last 90 Days': [moment().subtract(89, 'days'), moment()],
        'This Year': [moment().startOf('year'), moment()]
    }
});

//------

queue()
    .defer(d3.json, "/api/activities/" + document.getElementById('current-user').getAttribute('data-user') + "?date_start=" + moment().subtract(89, 'days').format('YYYY-MM-DD'))
    .await(Graph);

function Graph(error, recordsJson) {
    //load data
    var data = recordsJson;
    //define charts
    var pieChart = dc.pieChart("#pieChart");
    var numberChart = dc.numberDisplay("#numberChart");
    var timeChart = dc.barChart("#timeChart");

    // define interval select
    var intervals = {
        Days: d3.time.day,
        Weeks: d3.time.week,
        Months: d3.time.month
    };
    d3.select('#interval').selectAll('option')
        .data(Object.keys(intervals))
        .enter().append('option')
        .text(function(d) {
            return d;
        });
    var intervalString = d3.select('#interval')[0][0].value;
    var interval = intervals[intervalString];
    //define crossfilter
    var ndx = crossfilter(data);
    var dateFormat = d3.time.format("%Y-%m-%dT%H:%M:%SZ");
    var numberFormat = d3.format('.2f');

    //Define Dimension
    var typeDimension = ndx.dimension(function(d) {
        return d.type;
    });
    window.dateDim = ndx.dimension(function(d) {
        return interval(dateFormat.parse(d.date));
    });
    var gpxDim = ndx.dimension(function(d) {
        return d;
    });

    //Group Data
    var distanceGroup = typeDimension.group().reduceSum(function(d) {
        return d.distance;
    });
    var volumeByIntervalGroup = dateDim.group().reduceSum(function(d) {
        return d.distance;
    });
    var allGroup = ndx.groupAll().reduceSum(function(d) {
        return d.distance;
    });
    //min and max for X axis - if date range is outside of available data, use min and max from picker to draw axis
    var getMinDate = function() {
        if (dateDim.bottom(1).length > 0) {
            return dateFormat.parse(dateDim.bottom(1)[0].date);
        } else {
            return dateFormat.parse($('#datepicker').data('daterangepicker').startDate.format('YYYY-MM-DDT00:00:00') + 'Z');
        }
    };
    var getMaxDate = function() {
      if (dateDim.bottom(1).length > 0) {
          return dateFormat.parse(dateDim.top(1)[0].date);
      } else {
          return dateFormat.parse($('#datepicker').data('daterangepicker').endDate.format('YYYY-MM-DDT00:00:00') + 'Z');
      }
    };

    var updateData = function(ndx, dimensions) {
        var pieChartFilters = pieChart.filters();
        var timeChartFilters = timeChart.filters();
        pieChart.filter(null);
        timeChart.filter(null);
        ndx.remove();
        pieChart.filter([pieChartFilters]);
        timeChart.filter([timeChartFilters]);
    };

    var resetAll = function(data) {
        dc.filterAll(null);
        ndx.remove();
        ndx.add(data);
        timeChart
            .x(d3.time.scale().domain([interval.offset(getMinDate(), -1), interval.offset(getMaxDate(), 1)]))
            .xUnits(interval.range);

        if (intervalString == 'Days' && timeChart.data()[0].values.length > 21) {
            timeChart.xAxis().ticks(d3.time.week);
        } else {
            timeChart.xAxis().ticks(interval);
        }
        drawHeatMap();

    };

    var timeChartUpdate = function() {
        var updatedData = ndx.dimension(function(d) {
            return d;
        }).top(Infinity);
        updateData(ndx, [typeDimension, dateDim]);
        ndx.add(updatedData);
        intervalString = d3.select('#interval')[0][0].value;
        interval = intervals[intervalString];

        dateDim.dispose();
        dateDim = ndx.dimension(function(d) {
            return interval(dateFormat.parse(d.date));
        });
        volumeByIntervalGroup = dateDim.group().reduceSum(function(d) {
            return d.distance;
        });

        timeChart
            .filterAll()
            .dimension(dateDim)
            .group(volumeByIntervalGroup)
            .xUnits(interval.range)
            .x(d3.time.scale().domain([interval.offset(getMinDate(), -1), interval.offset(getMaxDate(), 1)]))
            .xAxis().ticks(interval);

        if (intervalString == 'Days' && timeChart.data()[0].values.length > 21) {
            timeChart.xAxis().ticks(d3.time.week);
        }

        dc.renderAll();
    };

    var drawHeatMap = function() {
        var gpxPoints = [];
        var latitude = [];
        var longitude = [];
        gpxRecords = gpxDim.top(Infinity);
        map.eachLayer(function(layer) {
            if (layer.hasOwnProperty('_heat')) {
                map.removeLayer(layer);
            }
        });
        for (var i = 0; i < gpxRecords.length; i++) {
            latitude.push(gpxRecords[i].latitude_median);
            longitude.push(gpxRecords[i].longitude_median);
            for (var j = 0; j < gpxRecords[i].gpx.length; j++) {
                gpxPoints.push(gpxRecords[i].gpx[j]);
            }
        }
        if (latitude.length > 0) {
            map.setView([median(latitude), median(longitude)], 13);
        }
        gpxPoints = gpxPoints.map(function(p) {
            return [p[0], p[1]];
        });

        var heat = L.heatLayer(gpxPoints, {
            radius: 5,
            blur: 6
        }).addTo(map);
    };

    var median = function(values) {
        values.sort(function(a, b) {
            return a - b;
        });
        var half = Math.floor(values.length / 2);
        if (values.length % 2)
            return values[half];
        else
            return (values[half - 1] + values[half]) / 2.0;
    };

    timeChart
        .width(document.getElementById('timeChartStage').offsetWidth)
        .height(200)
        .margins({
            top: 20,
            left: 40,
            right: 30,
            bottom: 45
        })
        .dimension(dateDim)
        .group(volumeByIntervalGroup)
        .centerBar(true)
        .xUnits(interval.range)
        .barPadding(0.2)
        .x(d3.time.scale().domain([interval.offset(getMinDate(), -1), interval.offset(getMaxDate(), 1)]))
        .elasticY(true)
        .yAxis().ticks(6).tickFormat(function(v) {
            return v + 'km';
        });

    if (intervalString == 'Days' && timeChart.data()[0].values.length > 21) {
        timeChart.xAxis().ticks(d3.time.week);

    } else {
        timeChart.xAxis().ticks(interval);
    }

    pieChart
        .width(200)
        .height(205)
        .innerRadius(50)
        .externalRadiusPadding(5)
        .dimension(typeDimension)
        .group(distanceGroup)
        .title(function(d) {
            return numberFormat(d.value);
        });

    numberChart
        .formatNumber(numberFormat)
        .valueAccessor(function(d) {
            return d;
        })
        .group(allGroup);


    // map definition
    window.map = L.map('map', {
        fullscreenControl: true
    });
    var tiles = L.tileLayer('http://{s}.tiles.wmflabs.org/bw-mapnik/{z}/{x}/{y}.png', {
        attribution: '&copy; <a href="http://osm.org/copyright">OpenStreetMap</a> contributors',
    }).addTo(map);



    //hide loading & draw charts
    loadingElements = document.getElementsByClassName('loading');
    for (var i = 0; i < loadingElements.length; i++) {
        loadingElements[i].className += ' hidden';
    }
    dc.renderAll();
    drawHeatMap();


    //interval select
    document.getElementById('interval').addEventListener('change', function(e) {
        timeChartUpdate();
        ga('send', 'event', 'Activity Dashboard', 'Interval Change', e.target.value);
    });
    //reset button
    document.getElementById('resetTimeChart').addEventListener('click', function() {
        resetAll(data);
        dc.renderAll();
        ga('send', 'event', 'Activity Dashboard', 'Charts Reset');
    });
    // map update when chart filtered
    var dcCharts = dc.chartRegistry.list();
    for (var k = 0; k < dcCharts.length; k++) {
        dcCharts[k].on("filtered", function(chart, filter) {
            drawHeatMap();
        });
    }
    // update chart width on window resize
    var originalWidth = window.innerWidth;
    window.addEventListener('resize', function(event) {
        if (this.innerWidth != originalWidth) {
            timeChart
                .width(document.getElementById('timeChartStage').offsetWidth)
                .render();
            ga('send', 'event', 'Activity Dashboard', 'Window Resize');
        }
    });

    // bootstrap needs jQuery anyway
    // function to load new data from API (new user or new date range)
    var getData = function(user_id, date_start, date_end) {
        $('#overlay').show();
        queue()
            .defer(d3.json, "/api/activities/" + user_id + '?date_start=' + date_start + '&date_end=' + date_end)
            .await(newUserData);

        function newUserData(error, recordsJson) {
            data = recordsJson;
            resetAll(data);
            $('#overlay').hide();
            dc.redrawAll();
            //drawHeatMap();
        }
    };

    // select user
    $('.dropdown-menu li a').on('click', function() {
        if ($('.navbar-toggle').css('display') == 'block') {
            $('.navbar-toggle').click();
        }
        var user_id = d3.select(this).attr('data-user');
        var user = d3.select(this).text();
        $('#current-user').attr('data-user', user_id);
        $('#current-user').html(user + ' <span class="caret">');
        setCookie('user', user_id, 90);
        var date_start = $('#datepicker').data('daterangepicker').startDate.format('YYYY-MM-DD');
        var date_end = $('#datepicker').data('daterangepicker').endDate.format('YYYY-MM-DD');
        getData(user_id, date_start, date_end);
        ga('send', 'event', 'Activity Dashboard', 'User Changed - ' + user_id);
    });

    // change date range
    $('#datepicker').on('apply.daterangepicker', function(ev, picker) {
        var date_start = picker.startDate.format('YYYY-MM-DD');
        var date_end = picker.endDate.format('YYYY-MM-DD');
        var user_id = document.getElementById('current-user').getAttribute('data-user');
        getData(user_id, date_start, date_end);
        ga('send', 'event', 'Activity Dashboard', 'Date Range Changed');
    });
}
