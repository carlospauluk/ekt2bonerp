<?php
require_once('application/views/templates/list_header.php');
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
