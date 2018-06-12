<h1>Total por Deptos</h1>

<?php
$this->form_components->alert_box("info");
$this->form_components->alert_box("warn");
$this->form_components->alert_box("error");
?>

<table class="table table-striped table-sm table-hover">
    <thead>
        <tr>
            <th>#</th>
            <th>Depto</th>
            <th>Total</th>
            <th>%</th>
            <th></th>
        </tr>
    </thead>
    <tbody>

        <?php
        $i = 0;
        foreach ($results as $item) {
            $i++;
            ?>

            <tr>
                <th scope="row"><?= $i; ?></th>
                <td><?= $item['depto'] ?></td>
                <td><val class="crsr-money"><?= $item['total'] ?></val></td>
                <td><val class="crsr-money"><?= $item['porcent'] ?></val></td>
            </tr>

<?php } ?>

</tbody>
</table>
