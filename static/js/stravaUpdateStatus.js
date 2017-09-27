function getStatus() {
    if (read_cookie('task_id')) {
        $.ajax({
            type: 'GET',
            url: '/status/' + read_cookie('task_id'),
            success: function(data, status, request) {
                if (data == 'SUCCESS') {
                    clearInterval(checkStatus);
                    $('#message-status').addClass('alert-success');
                    $('#message-text').html('Activities loaded, please <a href="javascript:window.location.reload();">refresh</a> the page');
                    $('#message-status').show();
                    delete_cookie('task_id');
                } else if (data == 'FAILURE') {
                    clearInterval(checkStatus);
                    $('#message-status').addClass('alert-danger');
                    $('#message-text').html('There was an error, please try again');
                    $('#message-status').show();
                    delete_cookie('task_id');
                }
            },
            error: function() {
                clearInterval(checkStatus);
                $('#message-status').addClass('alert-danger');
                $('#message-text').text('There was an error, please try again');
                $('#message-status').show();
                delete_cookie('task_id');
            }
        });
    } else {
        clearInterval(checkStatus);
    }
}


var read_cookie = function(key) {
    var result;
    return (result = new RegExp('(?:^|; )' + encodeURIComponent(key) + '=([^;]*)').exec(document.cookie)) ? (result[1]) : null;
}

var delete_cookie = function(name) {
    document.cookie = name + '=;expires=Thu, 01 Jan 1970 00:00:01 GMT;';
};

var checkStatus = setInterval(getStatus, 1000);
