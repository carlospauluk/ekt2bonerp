<ul class="nav nav-pills">




    <li class="nav-item">
        <button type="button" class="btn btn-primary" onclick="window.location.href = '<?= base_url('/fin/progrfinanc/view/' . $progr_financ_id . '/' . $mesano_anterior) ?>'">
            <i class="fa fa-backward align-middle" aria-hidden="true"></i>
            Anterior</button>&nbsp;
    </li>
    <li class="nav-item">
        <button type="button" class="btn btn-primary" onclick="window.location.href = '<?= base_url('/fin/progrfinanc/view/' . $progr_financ_id . '/' . $mesano_proximo) ?>'">
            <i class="fa fa-forward align-middle" aria-hidden="true"></i>
            Próximo</button>&nbsp;
    </li>
    <li class="nav-item">
        <button type="button" class="btn btn-primary" onclick="window.location.href = '<?= base_url('/fin/progrfinanc/gerar/' . $progr_financ_id . '/' . $mesano) ?>'">
            <i class="fa fa-cog align-middle" aria-hidden="true"></i>
            Gerar</button>&nbsp;
    </li>



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
            <th>Categoria</th>
            <th>Previsto</th>
            <th>Lançado</th>
            <th>Realizado</th>
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
                <td><a href="<?= base_url('/fin/movimentacao/listby/' . $mesano . '/' . $item['categ_codigo']) ?>"><?= $item['categoria'] ?></a></td>
                <td class="crsr-money"><?= $item['previsto'] ?></td>
                <td class="crsr-money"><?= $item['lancado'] ?></td>
                <td class="crsr-money"><?= $item['realizado'] ?></td>
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
