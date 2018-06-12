
<table class="table table-striped table-sm table-hover crsr-datatable">
    <thead>
        <tr>
            <th>#</th>
            <th>Carteira</th>
            <th>Descrição</th>
            <th>Parc 1</th>
            <th>Valor Parc</th>
            <th>Parc</th>
            <th>Valor Empr</th>
            <th>Total Dev</th>
            <th>Saldo Dev</th>
            <th>Dev AV</th>
            <th>Taxa</th>
            <th></th>
        </tr>
    </thead>
    <tbody>

        <?php
        $i = 0;
        foreach ($cgs['itens'] as $item) {
            $i++;
            ?>

            <tr>
                <th scope="row"><?= $i; ?></th>
                <td><?= $item['carteira'] ?></td>
                <td><?= $item['descricao'] ?></td>
                <td class="crsr-datetime2date"><?= $item['primeira_parcela'] ?></td>
                <td class="crsr-money"><?= $item['valor_parcela'] ?></td>
                <td style="text-align: right"><?= $item['qtde_parcelas_restantes'] ?> / <?= $item['qtde_parcelas_total'] ?></td>
                <td class="crsr-dec2"><?= $item['valor_emprestimo'] ?></td>
                <td class="crsr-dec2"><?= $item['valor_devedor_total'] ?></td>
                <td class="crsr-dec2"><?= $item['saldo_devedor'] ?></td>
                <td class="crsr-dec2"><?= $item['devedor_a_vista'] ?></td>
                <td class="crsr-dec3"><?= $item['taxa'] ?></td>
                <td></td>
            </tr>

        <?php } ?>            

    </tbody>

    <tfoot>
        <tr>
            <td colspan="4" style="text-align: right;">Total:</td>
            <td class="crsr-dec2"><?= $cgs['total_valor_parcelas'] ?></td>
            <td></td>
            <td class="crsr-dec2"><?= $cgs['total_valor_emprestimos'] ?></td>
            <td class="crsr-dec2"><?= $cgs['total_valor_devedor_total'] ?></td>
            <td class="crsr-dec2"><?= $cgs['total_saldo_devedor'] ?></td>
            <td class="crsr-dec2"><?= $cgs['total_devedor_a_vista'] ?></td>
            <td></td>
        </tr>
    </tfoot>

</table>
