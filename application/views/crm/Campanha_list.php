<nav class="navbar navbar-expand-lg navbar-light bg-light">
    <a class="navbar-brand" href="#">Campanhas</a>
    <button class="navbar-toggler" type="button" data-toggle="collapse" data-target="#navbarSupportedContent" aria-controls="navbarSupportedContent" aria-expanded="false" aria-label="Toggle navigation">
        <span class="navbar-toggler-icon"></span>
    </button>

    <div class="collapse navbar-collapse" id="navbarSupportedContent">
        <ul class="navbar-nav mr-auto">
            <li class="nav-item active">
                <button type="button" class="btn btn-primary" onclick="window.location.href = '<?php echo base_url('crm/Campanha/form'); ?>'">
                    <i class="fa fa-file-o align-middle" aria-hidden="true"></i>
                    Novo</button>&nbsp;
            </li>            
        </ul>
        <form class="form-inline my-2 my-lg-0">
            <input class="form-control mr-sm-2" type="text" placeholder="Pesquisar" aria-label="Pesquisar">
            <button class="btn btn-outline-success my-2 my-sm-0" type="submit">
                <i class="fa fa-search align-middle" aria-hidden="true"></i>
                Pesquisar</button>
            &nbsp;
            <button type="button" class="btn btn-secondary">
                <i class="fa fa-search-plus align-middle" aria-hidden="true"></i></button>

        </form>
    </div>
</nav>

<?php
    $this->form_components->alert_box("info");
    $this->form_components->alert_box("warn");
    $this->form_components->alert_box("error");
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
                            onclick="window.location.href = '<?php echo base_url('crm/Campanha/form/') . $item['id']; ?>'">
                        <i class="fa fa-wrench align-middle" aria-hidden="true"></i>
                    </button>&nbsp;
                    <button type="button" class="btn btn-danger btn-sm" data-toggle="modal" data-target="#confirmDlg"
                            onclick="set_del_url_confirmDlg('<?php echo base_url('crm/Campanha/del/') . $item['id']; ?>')">
                        <i class="fa fa-trash-o align-middle" aria-hidden="true"></i>
                    </button>

                </td>
            </tr>

        <?php } ?>

    </tbody>
</table>
