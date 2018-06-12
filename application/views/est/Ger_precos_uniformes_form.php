<div class="row">
    <div class="col-lg-12">
        <h1 class="page-header">Preços de Uniformes</h1>

        <form role="form" id="form1">


            <div class="form-group row">
                <label>Escola</label>
                <select class="form-control" id="i_escolas" name="i_escolas">            
                </select>
            </div>
            <div class="form-group row">
                <label>Tamanho</label>
                <select class="form-control" id="i_tamanhos" name="i_tamanhos">            
                </select>
            </div>
            <div class="form-group row">
                <label>Modelos</label>
                <div id="i_modelos">
                </div>
            </div>
            <div class="form-group row" id="div_table">
                <table class="table table-condensed">
                    <thead>
                        <tr>
                            <th>

                            </th>
                            <th>Descrição</th>
                            <th>Valor a Prazo</th>
                            <th>Valor a Vista</th>
                            <th>Preço Promo</th>
                            <th>Qtde</th>
                        </tr>
                    </thead>
                    <tbody id="table_body">
                        <tr>
                            <th></th>
                            <th></th>
                            <th></th>
                            <th></th>
                            <th></th>
                            <th></th>
                        </tr>
                    </tbody>

                </table>
            </div>

            <div class="form-group row">
                <button type="button" class="btn btn-primary" id="btn_gerar_mensagem">Gerar mensagem</button>
            </div>
            
            <div class="form-group row">
                <label>Mensagem</label>
                <textarea class="form-control" id="msg" name="msg" rows="15"></textarea>
            </div>

            <script>

                $(function () {
                    $.ajax({
                        type: "POST",
                        url: "<?= base_url("/est/gerprecosuniformes/get_escolas") ?>",
                        async: false,
                        success: function (result) {
                            var result = JSON.parse(result);
                            $("#i_escolas").append($("<option />"));
                            $.each(result, function (i, item) {
                                $("#i_escolas").append($("<option />").val(item.id).text(item.nome_fantasia));
                            });
                            $("#i_escolas").val();
                            $("#i_escolas").val($("#i_escolas option:first").val());
                            $("#i_escolas").val();
                        },
                        error: function () {
                            alert('ajax error');
                        }
                    });

                    $.ajax({
                        type: "POST",
                        url: "<?= base_url("/est/gerprecosuniformes/get_tamanhos") ?>",
                        async: false,
                        success: function (result) {
                            var result = JSON.parse(result);
                            // $("#i_tamanhos").append($("<option />"));
                            $.each(result, function (i, item) {
                                $("#i_tamanhos").append($("<option />").val(item.id).text(item.tamanho));
                            });
                            $("#i_tamanhos").val();
                            $("#i_tamanhos").val($("#i_tamanhos option:first").val());
                            $("#i_tamanhos").val();
                        },
                        error: function () {
                            alert('ajax error');
                        }
                    });


                    function build_modelos(result) {
                        var result = JSON.parse(result);
                        $("#i_modelos").empty();
                        $.each(result, function (i, item) {
                            $("#i_modelos").append("<div class=\"checkbox\">");
                            $("#i_modelos").append("<label>");
                            $("#i_modelos").append("<input type=\"checkbox\" name=\"i_modelos[]\" id=\"i_modelos\" value=\"" + item.id + "\" checked=\"checked\">" + item.subdepto);
                            $("#i_modelos").append("</label>");
                            $("#i_modelos").append("</div>");
                        });
                    }

                    function build_table(result) {
                        $("#table_body").empty();
                        var result = JSON.parse(result);
                        $.each(result, function (i, item) {
                            $("#table_body").append(
                                    "<tr>" +
                                    "<td><input type=\"checkbox\" name=\"i_produtos[" + i + "][check]\" id=\"i_produtos\" value=\"" + item.id + "\" checked=\"checked\"></td>" +
                                    "<td>" + item.descricao + "<input type='hidden' name='i_produtos[" + i + "][descricao]' value='" + item.descricao + "' /></td>" +
                                    "<td>" + item.preco_prazo + "<input type='hidden' name='i_produtos[" + i + "][preco_prazo]' value='" + item.preco_prazo + "' /></td>" +
                                    "<td>" + item.preco_vista + "<input type='hidden' name='i_produtos[" + i + "][preco_vista]' value='" + item.preco_vista + "' /></td>" +
                                    "<td>" + item.preco_promo + "<input type='hidden' name='i_produtos[" + i + "][preco_promo]' value='" + item.preco_promo + "' /></td>" +
                                    "<td>" + item.qtde + "</td>" +
                                    "</tr>");
                        });
                        // $("#i_produtos").click(function () { build_msg() }).click();
                    }
                    
                    $("#btn_gerar_mensagem").click(
                        
                            function () {
                                build_msg();
                            }
                            
                    );

                    function build_msg() {
                        $.ajax({
                            type: "POST",
                            url: "<?= base_url("/est/gerprecosuniformes/build_msg") ?>",
                            async: true,
                            data: $('#form1').serialize(),
                            success: function (result) {
                                $("#msg").val(result);
                            },
                            error: function () {
                                alert('ajax error');
                            }
                        });
                    }


                    $("#i_modelos").click(function () {
                        if ($("#i_escolas").val() && $("#i_tamanhos").val()) {
                            $.ajax({
                                type: "POST",
                                url: "<?= base_url("/est/gerprecosuniformes/get_produtos") ?>",
                                async: true,
                                data: $('#form1').serialize(),
                                success: function (result) {
                                    build_table(result)
                                },
                                error: function () {
                                    alert('ajax error');
                                }
                            });
                        }
                    });





                    $("#i_escolas")
                            .change(function () {
                                if ($("#i_escolas").val()) {
                                    var result = $.ajax({
                                        url: "<?= base_url("/est/gerprecosuniformes/get_subdeptos/") ?>"  + $("#i_escolas").val(),
                                        async: false
                                    }).responseText;
                                    build_modelos(result);
                                }
                                $("#i_modelos").trigger('click');

                            })
                            .change();

                    $("#i_tamanhos")
                            .change(function () {
                                $("#i_modelos").trigger('click');
                            })
                            .change();


                });

            </script>


        </form>

    </div>
</div>
