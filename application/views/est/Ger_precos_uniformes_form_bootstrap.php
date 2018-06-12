<h1>Preços Uniformes</h1>

<form>
    <div class="form-group row">
        <label class="col-sm-2 col-form-label" for="exampleFormControlSelect1">Escola</label>
        <select class="col-sm-10 form-control" id="select1">            
        </select>
    </div>
    <fieldset class="form-group">
        <div class="row" id="div_checkss">            
        </div>
    </fieldset>
    <script>

        $(function () {
            $.ajax({
                type: "POST",
                url: "http://localhost/crosier/est/gerprecosuniformes/get_escolas",
                async: false,
                success: function (result) {
                    var result = JSON.parse(result);
                    $.each(result, function (i, item) {
                        $("#select1").append($("<option />").val(item.id).text(item.nome_fantasia));                        
                    });
                    $("#select1").val();
                    $("#select1").val($("#select1 option:first").val());
                    $("#select1").val();
                },
                error: function () {
                    alert('ajax error');
                }
            });

            $("#select1")
                    .change(function () {
                        build("div_checkss")
                    })
                    .change();


        });


        function build(id) {
            
            var _values = $.ajax({                
                url: "http://localhost/crosier/est/gerprecosuniformes/get_subdeptos/" + $("#select1").val(),
                async: false
            }).responseText;
            
            $.ajax({
                type: "POST",
                url: "http://localhost/crosier/base/FC/checkbox/",
                data: { data : _values },
                success: function (result) {
                    $("#" + id).html(result);
                },
                error: function () {
                    alert('ajax error');
                }
            });



        }
    </script>


</form>