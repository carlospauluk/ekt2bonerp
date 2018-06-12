<?php echo form_open('crm/carteira/form'); ?>

<h1>Carteira</h1>

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
            <?php $this->form_components->input_datetime2date('dt_consolidado', 'Dt Consolidado', $dt_consolidado) ?>
        </div>        
    </div>

    
    <?php $this->form_components->spacer(20) ?>
       
    
    <div class="row">
        <div class="col" style="text-align: right">
            <input type="submit" name="submit" value="Salvar" class="btn btn-primary" />
        </div>
    </div>

</div>

</form>
