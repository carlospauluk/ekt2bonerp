<h1>Custos de Vendas</h1>

<?php
$this->form_components->alert_box("info");
$this->form_components->alert_box("warn");
$this->form_components->alert_box("error");
?>

<table class="table table-striped table-sm table-hover">

    <?php
    $i = 0;
    foreach ($data['results'] as $r) {
        $i++;
        ?>

<thead>
    <tr>
        <th>#<?= $i ?> (<?= $r['fornecedor']['id'] ?>)</th>
        <th>Fornecedor: <?= $r['fornecedor']['codigo'] . " - " . $r['fornecedor']['nome_fantasia'] ?></th>       
        <th>Total Custo: <?= $r['fornecedor']['total_preco_custo'] ?></th>       
        <th>Total Venda: <?= $r['fornecedor']['total_preco_venda'] ?></th>       
        <th>Total CMV: <?= $r['fornecedor']['cmv'] ?></th>       
    </tr>
    
    <?php
        foreach ($r['subdeptos'] as $subdepto) {
    ?>
    
    <tr>
        <td><?= $subdepto['subdepto_codigo'] . " - " . $subdepto['subdepto'] ?></td>
        <td>Vlr Custo:</td>
        <td>Vlr Venda:</td>
        <td>CMV:</td>
    </tr>
    
    <?php
        }
    ?>
    
</thead>    
    
    
    

<?php } ?>


