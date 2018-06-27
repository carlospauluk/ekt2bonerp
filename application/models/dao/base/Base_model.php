<?php
namespace CIBases\Models\DAO\Base;

use CIBases\Libraries\Input_formatter;
use CIBases\Libraries\Datetime_formatter;

class Base_model extends \CI_Model
{

    public $table;

    public $fields;

    public $formatters = array();

    public function __construct($table)
    {
        parent::__construct();
        $this->load->database();
        $this->table = $table;
        $this->fetch_fields();
    }

    public function findby_id($id)
    {
        $query = $this->db->get_where($this->table, array(
            'id' => $id
        ));
        $result = $query->result_array();
        if (count($result) > 1) {
            throw new Exception('Mais de um registro com o mesmo id.');
        } else if (count($result) == 1) {
            return $result[0];
        } else {
            return null;
        }
    }

    public function find_all()
    {
        $query = $this->db->get($this->table);
        return $query->result_array();
    }

    /**
     * Obtém os metadados dos campos.
     */
    public function fetch_fields()
    {
        $this->formatters = array();
        $this->fields = $this->db->field_data($this->table);
        foreach ($this->fields as $field) {
            $this->formatters[$field->name] = $field->type;
        }
        $this->load_formatters();
    }

    public function empty_record()
    {
        $data = array();
        foreach ($this->fields as $field) {
            $data[$field->name] = '';
        }
        return $data;
    }

    /**
     * Definido para ser sobrecarregado pelas classes filhas.
     */
    public function load_formatters()
    {}

    /**
     * Remonta o array $data para a exibição no formulário já com os dados formatados.
     *
     * @return type
     */
    public function inputpost2data()
    {
        $data = array();
        foreach ($this->fields as $field) {
            $value_formatted = Input_formatter::to_db($this->formatters[$field->name], $this->input->post("i_" . $field->name));
            $data[$field->name] = strtoupper($value_formatted);
        }
        return $data;
    }

    /**
     * Procedimentos padrões para salvar uma entidade na base de dados.
     *
     * @return boolean|int
     */
    public function save($data)
    {
        // Realiza as conversões
//         $data = $this->inputpost2data();
        $ret = null;
        $now = new \DateTime("now", new \DateTimeZone('- 0300'));
        // Métodos de controle
        $data['updated'] = $now->format('Y-m-d H:i:s');
        
        // @TODO: pegar estas informações a partir dos dados do usuário logado
        $data['estabelecimento_id'] = 1;
        $data['user_inserted_id'] = 1;
        $data['user_updated_id'] = 1;
        
        try {
            // Inicia a transação
            $this->db->trans_start();
            
            // Se passou o id, então é para UPDATE
            if ($data['id']) {
                $this->db->where('id', $data['id']);
                $ret = $this->db->update($this->table, $data);
                
                if ($ret) {
                    $this->db->trans_complete();
                    if ($this->db->trans_status() === FALSE) {
                        throw new Exception('Erro ao finalizar transação do UPDATE.');
                    } else {
                        return $data['id'];
                    }
                } else {
                    throw new Exception('Erro ao realizar o UPDATE.');
                }
            } else { // Se não passou id, é para INSERT
                $data['id'] = null;
                $data['inserted'] = $data['updated'];
                $ret = $this->db->insert($this->table, $data);
                if ($ret) {
                    $query = $this->db->query('SELECT LAST_INSERT_ID() as last_id');
                    $row = $query->row_array();
                    $last_id = $row['last_id'];
                    $this->db->trans_complete();
                    if ($this->db->trans_status() === FALSE) {
                        throw new Exception('Erro ao finalizar transação do INSERT.');
                    } else {
                        return $last_id;
                    }
                } else {
                    throw new Exception('Erro ao realizar o INSERT.');
                }
            }
        } catch (Exception $e) {
            log_message('error', $e->getMessage());
            log_message('error', $this->db->error()['message']);
            throw new Exception('Erro ao salvar o registro.', null, $e);
        }
    }

    public function delete($id)
    {
        $this->db->trans_start();
        $ret = $this->db->delete($this->table, array(
            'id' => $id
        ));
        $this->db->trans_complete();
        if ($this->db->trans_status() === FALSE) {
            log_message('error', $this->db->error()['message']);
            $this->session->set_flashdata('db_error_msg', $this->db->error()['message']);
            return false;
        } else {
            return $ret;
        }
    }
}
