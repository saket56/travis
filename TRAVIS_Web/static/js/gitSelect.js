$(document).ready(function () {
    $('input[name=flexRadioDefault]:radio').change(function (e) {
        let value = e.target.value.trim()
        $('[class^="form"]').css('display', 'none');
        switch (value) {
            case 'red':
                // $('.form-check').show()
                $('.form-a').show()
                break;
            case 'green':
                // $('.form-check').show()
                $('.form-b').show()
                break;
            default:
                break;
        }
    })
})