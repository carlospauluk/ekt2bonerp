<?php

class Gerprecosuniformes extends CI_Controller {

    public function __construct() {
        parent::__construct();
        $this->load->model('est/Ger_precos_uniformes_model');
        $this->model = $this->Ger_precos_uniformes_model;
    }

    public function get_escolas() {
        $r = $this->model->get_escolas();
        $r = json_encode($r);
        echo $r;
    }

    public function get_subdeptos($escolas_ids) {
        $r = $this->model->get_subdeptos($escolas_ids);
        echo json_encode($r);
    }

    public function get_tamanhos() {
        $r = $this->model->get_tamanhos();
        $r = json_encode($r);
        echo $r;
    }

    public function get_produtos() {

        $escola_id = $this->input->post("i_escolas");
        $tamanho_id = $this->input->post("i_tamanhos");
        $modelos_ids = $this->input->post("i_modelos");

        $r = $this->model->get_produtos($escola_id, $modelos_ids, $tamanho_id);
        $r = json_encode($r);
        
        echo $r;
    }

    public function build_msg() {
        // print_r($this->input->post("i_produtos"));
        // return;
        $produtos = $this->input->post("i_produtos");
        
        if (count($produtos) < 1) {
            echo "NADA";
            return;
        }
        
        
        echo "Então assim...\n\n";
        echo "O preço a prazo é para pagamento em até 6x sem juros nos cartões ou em nosso crediário!!\n";
        echo "Já o preço a vista é com 10% de desconto, para pagamentos em dinheiro, no cartão de débito ou no crédito em 1x!!\n\n";
        echo "Ah, e esses preços são da nossa *** promoção da TABELA 2016 *** (por favor, curta e compartilhe em seu facebook):\n";
        echo "https://www.facebook.com/casabonsucesso/videos/1355469564576492/\n\n";


        uasort($produtos, function($a, $b) {
            return strcmp($a["descricao"], $b["descricao"]);
        });

        foreach ($produtos as $produto) {
            if (isset($produto['check'])) {
                echo "- " . $this->ajustar_descricao($produto['descricao']) . ": R$ " . $produto['preco_prazo'] . " (A PRAZO) e R$ " . $produto['preco_vista'] . " (A VISTA)";
                if ($produto['preco_promo'] > 0) {
                    echo " >>> (PREÇO NA PROMOÇÃO: " . $produto['preco_promo'] . ")";
                }
                echo "\n";
            }
        }


        echo "\n\n\nFicamos na Avenida Dom Pedro II, 337, na Nova Rússia... em frente ao Shopping Total!\n";
        echo "De segunda a sexta ficamos abertos das 9h às 18h30... e nos sábados das 9h às 17h !!!\n\n";
        echo "Viu.. é bom vir logo, pois essa promoção aí da TABELA 2016 logo logo acaba, ta´?? ;-) :-D";
    }

    public function ajustar_descricao($descricao) {
        $de = array(" MC", " ML", " BCA", " PTO", " CNG", " FEC", " POLITEL");
        $para = array(" MANGA CURTA", " MANGA LONGA", " BRANCA", " PRETO", " CANGURU", " FECHADO", " ACTION");
        return str_replace($de, $para, $descricao);
    }

    public function ger_msg() {
        $this->load->view('templates/header_primeui');
        $this->load->view('est/Ger_precos_uniformes_form');
        $this->load->view('templates/footer_primeui');
    }

}
