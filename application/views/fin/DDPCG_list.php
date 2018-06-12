<ul class="nav nav-pills">

   

    <?php if ($method == 'listagrup') { ?>
        <li class="nav-item">
            <button type="button" class="btn btn-primary" onclick="window.location.href = '<?= base_url('/fin/ddpcg/listall/') . $carteira ?>'">
                <i class="fa fa-file-o align-middle" aria-hidden="true"></i>
                Todos</button>&nbsp;
        </li>
    <?php } ?>
    <?php if ($method == 'listall') { ?>
        <li class="nav-item">
            <button type="button" class="btn btn-primary" onclick="window.location.href = '<?= base_url('/fin/ddpcg/listagrup/') . $carteira ?>'">
                <i class="fa fa-file-o align-middle" aria-hidden="true"></i>
                Agrupado</button>&nbsp;
        </li>   
    <?php } ?>

    <li class="nav-item"><a class="nav-link" href="<?= base_url('/fin/ddpcg/' . $method . '/bradesco') ?>">Bradesco</a></li>
    <li class="nav-item"><a class="nav-link" href="<?= base_url('/fin/ddpcg/' . $method . '/sicredi') ?>">Sicredi</a></li>
    <li class="nav-item"><a class="nav-link" href="<?= base_url('/fin/ddpcg/' . $method . '/itau') ?>">Itaú</a></li>
    <li class="nav-item"><a class="nav-link" href="<?= base_url('/fin/ddpcg/' . $method . '/') ?>">Todos</a></li>
</ul>


<?php
$this->form_components->alert_box("info");
$this->form_components->alert_box("warn");
$this->form_components->alert_box("error");
?>


<table class="table table-striped table-sm table-hover">
    <thead>
        <tr>
            <th>#</th>
            <th>Banco</th>
            <th>Descrição</th>
            <th>Dt Vencto</th>
            <th>Valor</th>
            <th></th>
        </tr>
    </thead>
    <tbody>

        <?php
        $i = 0;
        foreach ($itens as $item) {
            $i++;
            ?>

            <tr>
                <th scope="row"><?= $i; ?></th>
                <td><?= $item['banco'] ?></td>
                <td><?= $item['descricao'] ?></td>
                <td class="crsr-datetime2date"><?= $item['dt_vencto'] ?></td>
                <td class="crsr-money"><?= $item['valor_total'] ?></td>
                <td></td>
            </tr>

        <?php } ?>            

    </tbody>

    <tfoot>
        <tr>
            <td colspan="4" style="text-align: right;">Total:</td>
            <td class="crsr-money"><?= $total ?></td>
            <td></td>
        </tr>
    </tfoot>

</table>
