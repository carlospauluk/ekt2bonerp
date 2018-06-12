<div class="row">
    <div class="col-lg-12">
        <h1 class="page-header">Preços de Uniformes</h1>

        <form role="form">


            <div class="form-group">
                <label>Escolas</label>
                <select class="form_control" id="i_escolas" name="i_escolas"></select>
            </div>

            <script>
                $('#i_escolas').puidropdown({
                    data: function (callback) {
                        $.ajax({
                            type: "GET",
                            url: '<?php echo base_url('/est/gerprecosuniformes/get_escolas') ?>',
                            dataType: "json",
                            context: this,
                            success: function (result) {

                                var r = [];
                                $.each(result, function (i, item) {
                                    r.push({"label": item.nome_fantasia,
                                        "value": item.id});

                                });
                                callback.call(this, r);
                            }
                        });
                    }
                });

            </script>


        </form>

    </div>
</div>
