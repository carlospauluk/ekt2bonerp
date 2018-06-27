<?php

/**
 * Job que realiza a importação dos fornecedores a partir da tabela ekt_fornecedor.
 */
class ImportarFornecedores extends CI_Controller
{

    private $inseridos;

    private $existentes;

    public function __construct()
    {
        parent::__construct();
        ini_set('max_execution_time', 0);
        ini_set('memory_limit', '2048M');
        $this->load->database();
        error_reporting(E_ALL);
        ini_set('display_errors', 1);
        $this->load->model('est/fornecedor_model');
    }

    /**
     */
    public function setar_codigos_ekt()
    {
        $time_start = microtime(true);
        $this->db->trans_start();
        
        $sql = "SELECT * FROM ekt_fornecedor WHERE nome_fantasia IS NOT NULL AND trim(nome_fantasia) != '' ORDER BY id";
        $query = $this->db->query($sql) or $this->exit_db_error();
        $result = $query->result_array();
        
        echo "<pre>";
        
        $i = 0;
        foreach ($result as $fornecedorEkt) {
            
            $nome = $fornecedorEkt['NOME_FANTASIA'];
            $nome = preg_replace("( )", "", $nome);
            echo "\n\n" . str_pad("", 150, "-") . " " . ++ $i . "\n";
            echo "INICIANDO... " . $nome . "\n";
            
            if (! $nome) {
                echo "Sem nome (código: " . $fornecedorEkt['CODIGO'] . ")\n\n";
                continue;
            }
            
            $query = $this->db->query("SELECT * FROM vw_est_fornecedor WHERE REPLACE(nome_fantasia,' ','') LIKE ? AND tipo = 'ESTOQUE'", array(
                $nome
            )) or $this->exit_db_error();
            $r = $query->result_array();
            
            if (count($r) > 1) {
                echo "\n\n" . str_pad("", 150, "%") . "\n";
                echo "Mais de um encontrado: " . $nome;
                echo "\n\n" . str_pad("", 150, "%") . "\n";
                continue;
            } else {
                if (count($r) == 1) {
                    echo "Somente um encontrado.\n";
                    $fornecedorBonERP = $r[0];
                    echo "OK\nSalvando na depara...";
                    $this->salvarNaDePara($fornecedorBonERP, $fornecedorEkt);
                    echo "OK.";
                } else {
                    echo "NENHUM ENCONTRADO... salvando...\n\n";
                    $this->salvarFornecedor($fornecedorEkt);
                }
            }
        }
        
        echo "FINALIZANDO.\n\n";
        
        $this->db->trans_complete();
        
        $time_end = microtime(true);
        $execution_time = ($time_end - $time_start);
        echo "\n\n\n\n----------------------------------\nTotal Execution Time: " . $execution_time . "s";
    }

    /**
     * Importa os fornecedores da ekt_fornecedor
     */
    public function importar($mesano)
    {
        $time_start = microtime(true);
        $this->db->trans_start();
        
        $this->mesano = $mesano;
        $this->dtMesano = DateTime::createFromFormat('Ymd', $mesano . "01");
        $this->dtMesano->setTime(0, 0, 0, 0);
        if (! $this->dtMesano instanceof DateTime) {
            die("mesano inválido.\n\n\n");
        }
        
        $sql = "SELECT * FROM ekt_fornecedor WHERE mesano = ? ORDER BY id";
        $query = $this->db->query($sql, $mesano) or $this->exit_db_error();
        $result = $query->result_array();
        
        echo "<pre>";
        // Para cada um dos ekt_fornecedor...
        foreach ($result as $fornecedorEkt) {
            
            $codigoEkt = $fornecedorEkt['CODIGO'];
            $nomeFantasia = trim($fornecedorEkt['NOME_FANTASIA']);
            
            // Pesquisa nos est_fornecedor pelo mesmo codigo_ekt, somente tipo 'ESTOQUE', e ainda vigentes (ate is NULL)
            $params = array(
                $codigoEkt,
                $nomeFantasia
            );
            $query = $this->db->query("SELECT * FROM vw_est_fornecedor WHERE codigo_ekt = ? AND nome_fantasia = ? AND tipo = 'ESTOQUE'", $params) or $this->exit_db_error();
            $r = $query->result_array();
            
            if (count($r) > 1) {
                die("Mais de um fornecedor encontrado com o mesmo código (" . $codigoEkt . ")");
            } else {
                // Não encontrou
                if (count($r) == 0) {
                    // Simplesmente salva
                    $this->salvarFornecedor($fornecedorEkt);
                } else {
                    // Já tem?
                    $fornecedorBonERP = $r[0];
                    // Verificar se é o mesmo
                    if ($this->checkMesmoNomeFantasia($fornecedorBonERP, $fornecedorEkt)) {
                        // se for, só atualiza
                        $this->salvarFornecedor($fornecedorEkt, $fornecedorBonERP);
                    } else { // é um fornecedor com mesmo código, porém com nome fantasia diferente
                        $this->salvarFornecedor($fornecedorEkt);
                    }
                }
            }
        }
        
        $this->db->trans_complete();
        
        echo "\n\n\nINSERIDOS: " . $this->inseridos;
        echo "\nEXISTENTES: " . $this->existentes;
        
        $time_end = microtime(true);
        $execution_time = ($time_end - $time_start);
        echo "\n\n\n\n----------------------------------\nTotal Execution Time: " . $execution_time . "s";
    }

    /**
     * Verifica se é o mesmo fornecedor comparando os nomes fantasias.
     *
     * @param
     *            $fornecedorBonERP
     * @param
     *            $fornecedorEkt
     * @return boolean
     */
    private function checkMesmoNomeFantasia($fornecedorBonERP, $fornecedorEkt)
    {
        $nomeFantasiaBonERP = preg_replace("[^A-Za-z]", "", $fornecedorBonERP['nome_fantasia']);
        $nomeFantasiaEkt = preg_replace("[^A-Za-z]", "", $fornecedorEkt['NOME_FANTASIA']);
        
        if (strcasecmp($nomeFantasiaBonERP, $nomeFantasiaEkt) == 0) {
            return true;
        } else {
            if ((strpos($nomeFantasiaBonERP, $nomeFantasiaEkt) !== false) || (strpos($nomeFantasiaEkt, $nomeFantasiaBonERP) !== false)) {
                return true;
            }
            return false;
        }
    }

    /**
     *
     * @param
     *            $fornecedorBonERP
     * @param
     *            $fornecedorEkt
     */
    private function salvarFornecedor($fornecedorEkt, $fornecedorBonERP = null)
    {
        $atualizando = false;
        $fornecedorBonERP_id = $fornecedorBonERP['id']; // pego o ID para os casos em que é uma atualização
        $pessoaId = isset($fornecedorBonERP['pessoa_id']) ? $fornecedorBonERP['pessoa_id'] : null;
        
        // $fornecedorBonERP = $this-> array(); // limpo o array pq ele na verdade veio da vw_est_fornecedor, portanto com campos a mais
        $fornecedorBonERP = $this->fornecedor_model->findby_id($fornecedorBonERP_id);
        
        if ($fornecedorBonERP_id) {
            $atualizando = true;
            // $fornecedorBonERP['codigo'] = $fornecedorEkt['CODIGO'];
            echo "ATUALIZANDO fornecedor... ";
        } else {
            echo "INSERINDO novo fornecedor... ";
            $fornecedorBonERP['codigo'] = $this->findNovoCodigo($fornecedorEkt['CODIGO']);
            $fornecedorBonERP['codigo_ekt_desde'] = $this->dtMesano->format('Y-m-d');
        }
        echo $fornecedorEkt['CODIGO'] . " - " . $fornecedorEkt['NOME_FANTASIA'] . "\n";
        
        // comentado para não setar aqui.. .só vai setar no salvarDePara
        // $fornecedorBonERP['codigo_ekt'] = $fornecedorEkt['CODIGO'];
        $fornecedorBonERP['inscricao_estadual'] = $fornecedorEkt['INSC'];
        $fornecedorBonERP['fone1'] = $fornecedorEkt['DDD_FONE'] . $fornecedorEkt['FONE'];
        $fornecedorBonERP['fone2'] = $fornecedorEkt['DDD_FAX'] . $fornecedorEkt['FAX'];
        $fornecedorBonERP['contato'] = $fornecedorEkt['CONTATO'];
        $fornecedorBonERP['representante'] = $fornecedorEkt['NOME_REPRES'];
        $fornecedorBonERP['representante_contato'] = $fornecedorEkt['DDD_REPRES'] . $fornecedorEkt['FONE_REPRES'];
        
        if ($atualizando) {
            $query = $this->db->get_where("bon_pessoa", array(
                'id' => $pessoaId
            )) or $this->exit_db_error();
            $r = $query->result_array();
            
            if (! $r) {
                die('bon_pessoa não encontrado: ' . $fornecedorBonERP['pessoa_id']);
            }
            $pessoa = $r[0];
        }
        
        $cnpj = preg_replace('/[^\d]/', '', $fornecedorEkt['CGC']); // remove tudo o que não for número
        $cnpj = sprintf("%014d", $cnpj); // preenche com zeros, caso não tenha 14 dígitos
        $nomeFantasia = $fornecedorEkt['NOME_FANTASIA'] ? $fornecedorEkt['NOME_FANTASIA'] : "";
        $razaoSocial = $fornecedorEkt['RAZAO'] ? $fornecedorEkt['RAZAO'] : $nomeFantasia;
        
        $pessoa['version'] = 0;
        $pessoa['updated'] = date("Y-m-d H:i:s");
        $pessoa['documento'] = $cnpj;
        $pessoa['nome'] = $razaoSocial;
        $pessoa['nome_fantasia'] = $nomeFantasia;
        $pessoa['tipo_pessoa'] = 'PESSOA_JURIDICA';
        
        if ($atualizando) {
            echo "Atualizando bon_pessoa... " . $pessoaId . "\n";
            $this->db->update('bon_pessoa', $pessoa, array(
                'id' => $pessoaId
            )) or $this->exit_db_error();
        } else {
            echo "Inserindo bon_pessoa... \n";
            $pessoa['inserted'] = date("Y-m-d H:i:s");
            $pessoa['estabelecimento_id'] = 1;
            $pessoa['user_inserted_id'] = 1;
            $pessoa['user_updated_id'] = 1;
            $this->db->insert('bon_pessoa', $pessoa) or $this->exit_db_error();
            $pessoaId = $this->db->insert_id();
        }
        
        $fornecedorBonERP['updated'] = date("Y-m-d H:i:s");
        $fornecedorBonERP['pessoa_id'] = $pessoaId;
        $fornecedorBonERP['fornecedor_tipo_id'] = 2;
        
        if ($atualizando) {
            echo "UPDATE est_fornecedor...\n";
            $this->db->update('est_fornecedor', $fornecedorBonERP, array(
                'id' => $fornecedorBonERP_id
            )) or $this->exit_db_error();
        } else {
            $fornecedorBonERP['version'] = 0;
            $fornecedorBonERP['inserted'] = date("Y-m-d H:i:s");
            $fornecedorBonERP['estabelecimento_id'] = 1;
            $fornecedorBonERP['user_inserted_id'] = 1;
            $fornecedorBonERP['user_updated_id'] = 1;
            if ($fornecedorEkt['id'] == 29) {
                echo "";
            }
            echo "INSERT na est_fornecedor... ";
            $this->db->insert('est_fornecedor', $fornecedorBonERP) or $this->exit_db_error();
            $fornecedorBonERP_id = $this->db->insert_id();
            echo $fornecedorBonERP_id . "\n";
        }
        // Agora já pode setar novamente
        $fornecedorBonERP['id'] = $fornecedorBonERP_id;
        
        $this->salvarNaDePara($fornecedorBonERP, $fornecedorEkt);
        
        $enderecoId = null;
        if ($atualizando) {
            // Pesquisa todos os endereços do fornecedor
            $sql = "SELECT e.* FROM est_fornecedor_enderecos fe, bon_endereco e WHERE fe.bon_endereco_id = e.id AND fe.est_fornecedor_id = ?";
            $params = array(
                $fornecedorBonERP_id
            );
            $query = $this->db->query($sql, $params) or $this->exit_db_error();
            $enderecos = $query->result_array();
            $enderecoId = null;
            if ($enderecos) {
                foreach ($enderecos as $endereco) {
                    if (trim($endereco['logradouro']) == trim($fornecedorEkt['ENDERECO'])) {
                        $enderecoId = $endereco['id'];
                        break;
                    }
                }
            }
        }
        
        $this->salvarEndereco($fornecedorEkt, $fornecedorBonERP_id, $enderecoId);
    }

    public function findNovoCodigo($codigoEkt)
    {
        $query = $this->db->get_where('est_fornecedor', array(
            'codigo' => $codigoEkt
        )) or $this->exit_db_error();
        if (count($query->result_array()) == 0) {
            return $codigoEkt;
        }
        
        $codigoStr = null;
        // codigo tem 9 dígitos (999999001)
        for ($f = 6; $f > 0; $f --) {
            $codigoStr = str_pad('', $f, "9", STR_PAD_LEFT) . str_pad($codigoEkt, (9 - $f), "0", STR_PAD_LEFT);
            
            $query = $this->db->get_where('est_fornecedor', array(
                'codigo' => $codigoStr
            )) or $this->exit_db_error();
            $existe = $query->result_array();
            
            if (count($existe) == 0) {
                return $codigoStr;
            }
        }
        
        die('Nenhum código (999999001) disponível para ' . $codigoEkt);
    }

    /**
     *
     * @param
     *            $fornecedorEkt
     * @param
     *            $fornecedorId
     * @param
     *            $enderecoId
     */
    private function salvarEndereco($fornecedorEkt, $fornecedorId, $enderecoId = null)
    {
        $endereco['logradouro'] = trim($fornecedorEkt['ENDERECO']);
        $endereco['numero'] = 0;
        $endereco['bairro'] = trim($fornecedorEkt['BAIRRO']);
        $endereco['cep'] = preg_replace('/[^\d]/', '', trim($fornecedorEkt['CEP']));
        $endereco['cidade'] = trim($fornecedorEkt['MUNICIPIO']);
        $endereco['estado'] = trim($fornecedorEkt['UF']);
        $endereco['tipoEndereco'] = 'COMERCIAL';
        $endereco['updated'] = date("Y-m-d H:i:s");
        
        if ($enderecoId) {
            $this->db->update('bon_endereco', $endereco, array(
                'id' => $enderecoId
            )) or $this->exit_db_error();
        } else {
            $endereco['version'] = 0;
            $endereco['inserted'] = date("Y-m-d H:i:s");
            $endereco['estabelecimento_id'] = 1;
            $endereco['user_inserted_id'] = 1;
            $endereco['user_updated_id'] = 1;
            
            $this->db->insert('bon_endereco', $endereco) or $this->exit_db_error();
            
            $enderecoId = $this->db->insert_id();
        }
        
        // many-to-many
        $query = $this->db->get_where("est_fornecedor_enderecos", array(
            'est_fornecedor_id' => $fornecedorId,
            'bon_endereco_id' => $enderecoId
        )) or $this->exit_db_error();
        $r = $query->result_array();
        
        if (count($r) == 0) {
            $est_fornecedor_enderecos['est_fornecedor_id'] = $fornecedorId;
            $est_fornecedor_enderecos['bon_endereco_id'] = $enderecoId;
            $this->db->insert('est_fornecedor_enderecos', $est_fornecedor_enderecos) or $this->exit_db_error();
        }
    }

    /**
     *
     * @param
     *            $fornecedorBonERP
     * @param
     *            $fornecedorEkt
     */
    private function salvarNaDePara($fornecedorBonERP, $fornecedorEkt)
    {
        echo "LIDANDO COM 'depara'... \n";
        $fornecedorId = $fornecedorBonERP['id'];
        
        $sql = "SELECT * FROM est_fornecedor_codektmesano WHERE fornecedor_id = ? AND mesano = ? AND codigo_ekt = ?";
        $params = array(
            $fornecedorId,
            $this->mesano,
            $fornecedorEkt['CODIGO']
        );
        $r = $this->db->query($sql, $params)->result_array();
        
        if (count($r) > 0) {} else {
            $codektmesano['fornecedor_id'] = $fornecedorId;
            $codektmesano['mesano'] = $this->mesano;
            $codektmesano['codigo_ekt'] = $fornecedorEkt['CODIGO'];
            
            $this->db->insert('est_fornecedor_codektmesano', $codektmesano);
        }
        
        // verifica se o mesano é menor que o codigo_ekt_desde
        // se for, seta o codigo_ekt_desde pro mesano
        if (array_key_exists('codigo_ekt_desde', $fornecedorBonERP) and $fornecedorBonERP['codigo_ekt_desde']) {
            $dt_cod_ekt_desde = DateTime::createFromFormat('Y-m-d', $fornecedorBonERP['codigo_ekt_desde']);
            $dt_cod_ekt_desde->setTime(0, 0, 0, 0);
            if ($this->dtMesano < $dt_cod_ekt_desde) {
                $fornecedorBonERP['codigo_ekt_desde'] = $this->dtMesano->format('Y-m-d');
                $this->db->update('est_fornecedor', $fornecedorBonERP, array('id' => $fornecedorBonERP['id'])) or die("Erro ao atualizar 'codigo_ekt_desde'");
            }
        }
        
        // verifica se o mesano é maior que o codigo_ekt_ate
        // se for, seta o codigo_ekt_ate pro mesano
        if (array_key_exists('codigo_ekt_ate', $fornecedorBonERP) and $fornecedorBonERP['codigo_ekt_ate']) {
            $dt_cod_ekt_ate = DateTime::createFromFormat('Y-m-d', $fornecedorBonERP['codigo_ekt_ate']);
            $dt_cod_ekt_ate->setTime(0, 0, 0, 0);
            if ($this->dtMesano > $dt_cod_ekt_ate) {
                $fornecedorBonERP['codigo_ekt_ate'] = $this->dtMesano->format('Y-m-d');
                $this->db->update('est_fornecedor', $fornecedorBonERP, array('id' => $fornecedorBonERP['id'])) or die("Erro ao atualizar 'codigo_ekt_ate'");
            }
        }
        
        echo "OK.\n\n";
    }

    /**
     *
     * @param
     *            $fornecedorBonERP
     * @param
     *            $fornecedorEkt
     */
    private function salvarNaDePara_old($fornecedorBonERP, $fornecedorEkt)
    {
        echo "LIDANDO COM a 'depara'... \n";
        $fornecedorId = $fornecedorBonERP['id'];
        
        $desde = DateTime::createFromFormat('Ymd', $fornecedorEkt['mesano'] . "01")->format('Y-m-d');
        
        // Verifica quantos registros existem com este codigo_ekt e 'ate' NULL
        $codigoEkt = $fornecedorEkt['CODIGO'];
        $sqlCheck = "SELECT id FROM est_fornecedor WHERE codigo_ekt_ate IS NULL AND codigo_ekt = ?";
        $query = $this->db->query($sqlCheck, array(
            $codigoEkt
        )) or $this->exit_db_error();
        $outros = $query->result_array();
        
        // não pode ter dois registros com mesmo codigo_ekt e ate=null
        if (count($outros) > 1) {
            die("ERRO NA BASE: Para o codigo_ekt " . $codigoEkt . " existe mais de um registro com ate=null");
        }
        // Se ainda não tinha o codigo_ekt na est_fornecedor_depara
        if (count($outros) == 0) {
            // Somente insere.
            echo "Não tinha ainda... só INSERT.\n";
            $sqlInsert = "UPDATE est_fornecedor SET codigo_ekt = ? , codigo_ekt_desde = ? WHERE id = ?";
            $this->db->query($sqlInsert, array(
                $fornecedorEkt['CODIGO'],
                $desde,
                $fornecedorId
            )) or $this->exit_db_error();
        } else {
            
            // Se já tinha o codigo_ekt na est_fornecedor_depara
            echo "Já tinha este codigo_ekt... ";
            
            // Porém não é o mesmo 'fornecedor_id'...
            if ($outros[0]['id'] != $fornecedorId) {
                echo "mas não é o mesmo fornecedor_id... ATUALIZANDO o anterior... ";
                // Atualizar o registro anterior marcando o 'ate'...
                $sql = "UPDATE est_fornecedor SET codigo_ekt_ate = ? WHERE id = ?";
                $ultimoDiaMesPassado = DateTime::createFromFormat('Ym', $fornecedorEkt['mesano'])->modify('-1 month')->format('Y-m-t');
                $this->db->query($sql, array(
                    $ultimoDiaMesPassado,
                    $outros[0]['id']
                )) or $this->exit_db_error();
                // E insere um novo registro setando apenas o 'desde'.
                echo "E inserindo um novo registro ... \n";
                $sqlInsert = "UPDATE est_fornecedor SET codigo_ekt = ? , codigo_ekt_desde = ? WHERE id = ?";
                $this->db->query($sqlInsert, array(
                    $fornecedorEkt['CODIGO'],
                    $desde,
                    $fornecedorId
                )) or $this->exit_db_error();
            }
        }
        
        echo "OK.\n\n";
    }

    /**
     *
     * @param type $codigoEkt
     * @return type
     */
    public function findFornecedoresAntigos($codigoEkt)
    {
        $sql = "SELECT * FROM vw_est_fornecedor WHERE codigo LIKE ?";
        $params = array(
            '999%' . sprintf("%03d", $codigoEkt)
        );
        $query = $this->db->query($sql, $params) or $this->exit_db_error();
        return $query->result_array();
    }

    private function exit_db_error()
    {
        echo str_pad("", 100, "*") . "\n";
        echo "LAST QUERY: " . $this->db->last_query() . "\n\n";
        print_r($this->db->error());
        echo str_pad("", 100, "*") . "\n";
        exit();
    }

    public function teste()
    {}
}
