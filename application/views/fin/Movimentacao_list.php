<?php
require_once('application/views/templates/list_header.php');
?>

<table class="table table-striped table-sm table-hover">
    <thead>
        <tr>
            <th>ID</th>
            <th>Descrição</th>
            <th>Dt</th>
            <th>Status</th>
            <th>Valor</th>
            <th>Carteira</th>
            <th>Categoria</th>            
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
                <th scope="row"><?= $item['id']; ?></th>
                <td><?= $item['descricao'] ?></td>
                <td class=""><?= $item['dt_util'] ?></td>
                <td><?= $item['status'] ?></td>
                <td class="crsr-money">
                        
                        
                        <?= $item['valor_total'] ?>
                
                </td>
                <td><?= $item['cart_codigo'] . " - " . $item['cart_descricao'] ?></td>
                <td><?= $item['categ_codigo'] . " - " . $item['categ_descricao'] ?></td>
                <td>
                    <button type="button" class="btn btn-primary btn-sm" 
                            onclick="window.location.href = '<?php echo base_url('financeiro/carteira/form/') . $item['id']; ?>'">
                        <i class="fa fa-wrench align-middle" aria-hidden="true"></i>
                    </button>&nbsp;
                    <button type="button" class="btn btn-danger btn-sm" data-toggle="modal" data-target="#confirmDlg"
                            onclick="set_del_url_confirmDlg('<?php echo base_url('financeiro/carteira/del/') . $item['id']; ?>')">
                        <i class="fa fa-trash-o align-middle" aria-hidden="true"></i>
                    </button>

                </td>
            </tr>

        <?php } ?>

    </tbody>
</table>
