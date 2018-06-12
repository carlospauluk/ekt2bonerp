
function set_del_url_confirmDlg(url) {
    $('#confirmDlg_btn_s').click(function () {
        window.location.href = url;
    });
}

$(document).ready(function () {

    $('.crsr-datatable').each(function () {
        if ($.fn.dataTable.isDataTable(this)) {
            table = this.DataTable();
        } else {
            $(this).DataTable({
                paging: false
            });
        }
    });


});