<?php
require_once('application/views/templates/list_header.php');
?>

<table class="table table-striped table-sm table-hover">
    <thead>
        <tr>
            <th>#</th>
            <th>Descrição</th>
            <th>Início</th>
            <th>Fim</th>
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
                <td><?= $item['descricao'] ?></td>
                <td class="crsr-datetime2date"><?= $item['dt_inicio'] ?></td>
                <td class="crsr-datetime2date"><?= $item['dt_fim'] ?></td>
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
