function formatInputs() {
    formatDateTimes();
    formatMoneys();
    formatDecs();
}

function formatMoneys() {

    $(".crsr-money").each(function () {
        var value;

        value = $(this).html();



        $(this).val(value);
        $(this).maskMoney({prefix: '', thousands: '.', decimal: ',', affixesStay: true, precision: 2});
        $(this).maskMoney('mask');


        // if ($(this).is('td')) {
        $(this).html($(this).val());
        // } else {
        //     $(this).val(formatted);
        // }
    });


//    $(".crsr-money").maskMoney({thousands: '.', decimal: ',', allowZero: true})
}

function formatDecs() {


    function _formatDec(_precis, thiz) {
        var value;
        if ($(thiz).is('td')) {
            value = $(thiz).html();
        } else {
            value = thiz.value;
        }

        value = parseFloat(Math.round(value * 100) / 100).toFixed(_precis);
        $(thiz).val(value);

        $(thiz).maskMoney({prefix: '', thousands: '.', decimal: ',', affixesStay: true, precision: _precis});
        $(thiz).maskMoney('mask');


        if ($(thiz).is('td')) {
            $(thiz).html($(thiz).val());
        } else {
            // $(this).val(formatted);
        }
    }

    $(".crsr-dec2").each(function () {
        _formatDec(2, this);
    });

    $(".crsr-dec3").each(function () {
        _formatDec(3, this);
    });

    $(".crsr-dec4").each(function () {
        _formatDec(4, this);
    });


}






function formatDateTimes() {

    $(".crsr-datetime").each(function () {
        var value;
        if ($(this).is('td')) {
            value = $(this).html();
        } else {
            value = this.value;
        }
        var formatted = moment(value, "YYYY-MM-DD HH:mm:ss").format("DD/MM/YYYY HH:mm:ss");
        if ($(this).is('td')) {
            $(this).html(formatted);
        } else {
            $(this).val(formatted);
        }
    });

    $(".crsr-datetime2date").each(function () {
        var value;
        if ($(this).is('td')) {
            value = $(this).html();
        } else {
            value = this.value;
        }
        var formatted = moment(value, "YYYY-MM-DD HH:mm:ss").format("DD/MM/YYYY");
        if ($(this).is('td')) {
            $(this).html(formatted);
        } else {
            $(this).val(formatted);
        }
    });
}

// -------------------------------------------------------

function maskInputs() {
    $('.crsr-datetime2date').mask('00/00/0000', {clearIfNotMatch: true, selectOnFocus: true});

    $('.crsr-money').maskMoney({
        precision: 2,
        allowZero: true,
        allowNegative: true
    });

    $('.crsr-input-dec3').maskMoney({
        precision: 3,
        allowZero: true,
        allowNegative: true
    });


}


$(document).ready(function () {
    formatInputs();
    maskInputs();
});

