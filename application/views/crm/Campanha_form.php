<?php // echo validation_errors();     ?>

<?php echo form_open('crm/campanha/form'); ?>

<h1>Campanha</h1>

<?php
    $this->form_components->alert_box("info");
    $this->form_components->alert_box("warn");
    $this->form_components->alert_box("error");
?>

<div class="container-fluid">

    <div class="row">
        <div class="col col-2"> 
            <div class="form-group">
                <?php $this->form_components->input_id($id) ?>
            </div>
        </div>
        <div class="col col-10">
            <div class="form-group">
                <?php $this->form_components->input_text('descricao', 'Descrição', $descricao) ?>
            </div>
        </div>
    </div>

    <div class="row">
        <div class="col">
            <?php $this->form_components->input_datetime2date('dt_inicio', 'Dt Início', $dt_inicio) ?>
        </div>
        <div class="col">
            <?php $this->form_components->input_datetime2date('dt_fim', 'Dt Fim', $dt_fim) ?>
        </div>
    </div>

    <div class="row">
        <div class="col">
            <?php $this->form_components->input_textarea('msg_cupom', 'Mensagem do Cupom', $msg_cupom) ?>
        </div>
    </div>

    <div class="row">
        <div class="col">
            <?php $this->form_components->input_textarea('obs', 'Obs', $obs) ?>
        </div>
    </div>

    <div class="row">
        <div class="col">&nbsp;
        </div>
    </div>

    <div class="row">
        <div class="col" style="text-align: right">
            <input type="submit" name="submit" value="Salvar" class="btn btn-primary" />
        </div>
    </div>

</div>

</form>
