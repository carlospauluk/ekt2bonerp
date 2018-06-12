<?php

class Inventario extends CI_Controller {

    public function __construct() {
        parent::__construct();
        $this->load->model('est/Inventario_model');
        $this->model = $this->Inventario_model;
    }

    public function total_por_deptos() {
        $this->data = $this->model->total_por_deptos();
               
        $this->load->view('templates/header');
        $this->load->view('est/Inventario_total_por_deptos', $this->data);
        $this->load->view('templates/footer');
    }
    
}
