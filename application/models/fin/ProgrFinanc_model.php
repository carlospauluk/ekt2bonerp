<?php

class ProgrFinanc_model extends CI_Model {

    public function __construct() {
        parent::__construct();
//         $this->load->database();
    }

    public function find_abertas_e_previstas($mesano) {
        // pesquisa todas as movimentações que estão no status ABERTA, A_COMPENSAR, PREVISTA
        // movimentacao.status in ('ABERTA','A_COMPENSAR','PREVISTA')
        $sql = "SELECT * FROM vw_fin_movimentacao "
                . "WHERE DATE_FORMAT(dt_vencto_efetiva,'%Y%m') = ?' AND "
                . "status in ('ABERTA','A_COMPENSAR','PREVISTA') "
                . "ORDER BY dt_vencto_efetiva, valor_total";
        $query = $this->db->query($sql, $mesano);
        return $query->result_array();
    }

    public function gerar($progr_financ_id, $mesano, $reset) {
        // depois ver para incluir transação aqui
        // verifica se já existe uma fin_progr_financ_mesano para esta
        if ($reset) {
            // apaga tudo
        }

        // Verifica se já existe uma fin_progr_financ_mesano
        $query = $this->db->query("SELECT id FROM fin_progr_financ_mesano "
                . "WHERE progr_financ_id = ? AND "
                . "DATE_FORMAT(mesano,'%Y%m') = ?", array($progr_financ_id, $mesano));
        $result = $query->result_array();
        if (count($result) == 0) {
            // se não achou insere
            $data = Entities::default_values();
            $data['mesano'] = DateTime::createFromFormat('Ymd', $mesano . "01")->format('Y-m-d');
            $data['progr_financ_id'] = $progr_financ_id;
            $progr_financ_mesano_id = Db_util::insert($this->db, 'fin_progr_financ_mesano', $data);
        } else {
            $progr_financ_mesano_id = $result[0]['id'];
        }

        $this->gerar_itens($progr_financ_mesano_id, $mesano);
    }

    /**
     * 
     * @param type $progr_financ_mesano_id
     * @throws Exception
     */
    public function gerar_itens($progr_financ_mesano_id, $mesano) {
        try {
            $this->db->trans_start();

            // Pega todas as categorias totalizáveis
            $query = $this->db->query("SELECT id, codigo FROM fin_categoria WHERE totalizavel IS TRUE ORDER BY codigo_ord");
            $categs = $query->result_array();

            foreach ($categs as $categ) {
                // Pega os abertos
                $sql_prevs = "SELECT sum(valor_total) as abertas FROM fin_movimentacao m, fin_categoria c "
                        . "WHERE m.categoria_id = c.id AND c.codigo LIKE ? AND "
                        . "status IN ('ABERTA','A_COMPENSAR','PREVISTA') AND "
                        . "DATE_FORMAT(dt_util,'%Y%m') = ?";
                $query = $this->db->query($sql_prevs, array($categ['codigo'] . "%", $mesano));
                $r_prevs = $query->result_array();

                // Pega os já realizados
                $sql_realiz = "SELECT sum(valor_total) as realizadas FROM fin_movimentacao m, fin_categoria c "
                        . "WHERE m.categoria_id = c.id AND c.codigo LIKE ? AND "
                        . "status IN ('REALIZADA') AND "
                        . "DATE_FORMAT(dt_util,'%Y%m') = ?";
                $query = $this->db->query($sql_realiz, array($categ['codigo'] . "%", $mesano));
                $r_realiz = $query->result_array();


                // Verifica se já tem um fin_progr_financ_mesano_item
                $query = $this->db->query("SELECT * FROM fin_progr_financ_mesano_item "
                        . "WHERE categoria_id = ? AND "
                        . "progr_financ_mesano_id = ?", array($categ['id'], $progr_financ_mesano_id));
                $r = $query->result_array();

                $data = array();
                $data['lancado'] = $r_prevs[0]['abertas'];
                $data['realizado'] = $r_realiz[0]['realizadas'];
                $data['progr_financ_mesano_id'] = $progr_financ_mesano_id;
                $data['categoria_id'] = $categ['id'];

                if (count($r) == 0) {
                    $data = array_merge($data, Entities::default_values());
                    $data['previsto'] = $r_prevs[0]['abertas'];

                    Db_util::insert($this->db, 'fin_progr_financ_mesano_item', $data);
                } else {
                    $data = array_merge($r[0], $data);
                    $data = Entities::upd($data);
                    $this->db->update('fin_progr_financ_mesano_item', $data, array('id' => $data['id']));
                }
            }

            $this->db->trans_complete();
            if ($this->db->trans_status() === FALSE) {
                throw new Exception('Erro ao finalizar transação.');
            }
        } catch (Exception $e) {
            log_message('error', $e->getMessage());
            log_message('error', $this->db->error()['message']);
            throw new Exception('Erro ao salvar o registro.', null, $e);
        }
    }

    /**
     * 
     * @param type $progr_financ_id
     * @param type $mesano
     * @return string
     */
    public function view($progr_financ_id, $mesano) {

        $query = $this->db->query("SELECT id, codigo, descricao FROM fin_categoria WHERE totalizavel IS TRUE ORDER BY codigo_ord");
        $categs = $query->result_array();

        foreach ($categs as $categ) {

            $item['categoria'] = $categ['codigo'] . " - " . $categ['descricao'];
            $item['categ_codigo'] = $categ['codigo'];

            $query = $this->db->query("SELECT * FROM fin_progr_financ_mesano_item mi, fin_progr_financ_mesano m "
                    . "WHERE mi.progr_financ_mesano_id = m.id AND "
                    . "categoria_id = ? AND "
                    . "m.progr_financ_id = ? AND "
                    . "DATE_FORMAT(m.mesano,'%Y%m') = ?", array($categ['id'],$progr_financ_id,$mesano));
            $mi = $query->result_array();
            
            $item['previsto'] = $mi[0]['previsto'];
            $item['lancado'] = $mi[0]['lancado'];
            $item['realizado'] = $mi[0]['realizado'];

            $r['itens'][] = $item;
        }

        return $r;
    }

    /**
     * 
     * @param type $progr_financ_id
     * @param type $mesano
     * @return type
     */
    public function find_by($progr_financ_id, $mesano) {
        $query = $this->db->query("SELECT id FROM fin_progr_financ_mesano "
                . "WHERE progr_financ_id = ? AND "
                . "DATE_FORMAT(mesano,'%Y%m') = ?'", $mesano);
        $result = $query->result_array();
        if (count($result) == 0) {
            $this->gerar($progr_financ_id, $mesano);
            // Chama recursivo, pois já foi gerado.
            return $this->find_by($progr_financ_id, $mesano);
        } else {
            $progr_financ_mesano_id = $result[0]['id'];

            $query = $this->db->query("SELECT "
                    . "i.categoria_id, "
                    . "c.codigo as categ_codigo, "
                    . "c.descricao as categ_descricao, "
                    . "i.previsto, "
                    . "i.lancado, "
                    . "i.realizado "
                    . "FROM fin_progr_financ_mesano_item i, fin_categoria c "
                    . "WHERE p.categoria_id = c.id "
                    . "AND progr_financ_mesano_id = ? AND "
                    . "DATE_FORMAT(mesano,'%Y%m') = ?'", array($progr_financ_mesano_id, $mesano));
            $result = $query->result_array();
        }
    }

    public function corrr() {

        $this->db->trans_start();

        $query = $this->db->query("SELECT id, codigo FROM fin_categoria");
        $categs = $query->result_array();

        foreach ($categs as $categ) {
            $codigo_ord = str_pad($categ['codigo'], 12, "0");

            $data['codigo_ord'] = $codigo_ord;

            $this->db->query("UPDATE fin_categoria SET codigo_ord = ? WHERE id = ?", array($codigo_ord, $categ['id']));

            // $ret = $this->db->update('fin_categoria', $data, array('id' => $categ['id']));
            // echo $ret . "<br>";
        }

        $this->db->trans_complete();

        if ($this->db->trans_status() === FALSE) {
            throw new Exception('Erro ao finalizar transação do UPDATE.');
        }
    }

    public function bla() {
        echo "bla";
        echo "";
    }

    public function teste() {

        $this->db->trans_start(FALSE);

        $qry = $this->db->query("SELECT * FROM teste");
        $r = $qry->result_array();

        echo count($r) . "<br>";

        $data['campo1'] = 'bleuyuiyi';
        $this->db->insert('teste', $data);

        $qry = $this->db->query("SELECT * FROM teste");
        $r = $qry->result_array();

        echo count($r) . "<br>";

        $this->db->trans_complete();
    }

    public function ii() {
        $data['campo1'] = 'ble';
        $this->db->insert('teste', $data);
    }

}
