<?php

/**
 * Job que realiza a importação dos fornecedores a partir da tabela ekt_fornecedor.
 */
class ImportarFornecedores extends CI_Controller
{

    private $inseridos = 0;

    private $atualizados = 0;

    private $acertados_depara = 0;

    /**
     * Conexão ao db ekt.
     */
    private $dbekt;

    /**
     * Conexão ao db bonerp.
     */
    private $dbbonerp;

    public function __construct()
    {
        parent::__construct();
        ini_set('max_execution_time', 0);
        ini_set('memory_limit', '2048M');
        
        $this->dbekt = $this->load->database('ekt', TRUE);
        $this->dbbonerp = $this->load->database('bonerp', TRUE);
        
        error_reporting(E_ALL);
        ini_set('display_errors', 1);
    }

    /**
     * Importa os fornecedores da ekt_fornecedor
     */
    public function importar($mesano)
    {
        $time_start = microtime(true);
        $this->dbbonerp->trans_start();
        
        $this->load->model('est/fornecedor_model');
        $this->fornecedor_model->setDb($this->dbbonerp);
        
        $this->mesano = $mesano;
        $this->dtMesano = DateTime::createFromFormat('Ymd', $mesano . "01");
        $this->dtMesano->setTime(0, 0, 0, 0);
        if (! $this->dtMesano instanceof DateTime) {
            die("mesano inválido.\n\n\n");
        }
        
        $sql = "SELECT * FROM ekt_fornecedor WHERE mesano = ? ORDER BY id";
        $query = $this->dbekt->query($sql, $mesano) or $this->exit_db_error();
        $result = $query->result_array();
        
        echo "<pre>";
        // Para cada um dos ekt_fornecedor...
        foreach ($result as $fornecedorEkt) {
            
            $codigoEkt = $fornecedorEkt['CODIGO'];
            $nomeFantasia = trim($fornecedorEkt['NOME_FANTASIA']);
            
            echo PHP_EOL . "EKT >>>>> Código: [" . $codigoEkt . "] Nome Fantasia: [" . $nomeFantasia . "]" . PHP_EOL;
            
            // Pesquisa nos est_fornecedor pelo mesmo codigo_ekt, somente tipo 'ESTOQUE', e ainda vigentes (ate is NULL)
            $params = array(
                $codigoEkt,
                $nomeFantasia
            );
            $query = $this->dbbonerp->query("SELECT * FROM vw_est_fornecedor WHERE codigo_ekt = ? AND nome_fantasia = ? AND tipo = 'ESTOQUE'", $params) or $this->exit_db_error();
            $r = $query->result_array();
            
            if (count($r) > 1) {
                die("Mais de um fornecedor encontrado com o mesmo código (" . $codigoEkt . ")");
            } else {
                // Não encontrou
                if (count($r) == 0) {
                    // Simplesmente salva
                    echo "Não existente. Salvando..." . PHP_EOL;
                    $this->salvarFornecedor($fornecedorEkt);
                    $this->inseridos ++;
                } else {
                    $fornecedorBonERP = $r[0];
                    echo "Já existente. Atualizando..." . PHP_EOL;
                    $this->salvarFornecedor($fornecedorEkt, $fornecedorBonERP);
                    $this->atualizados ++;
                }
                echo "OK!!!" . PHP_EOL;
            }
        }
        
        $this->dbbonerp->trans_complete();
        
        echo PHP_EOL . PHP_EOL . PHP_EOL;
        echo "--------------------------------------------------------------" . PHP_EOL;
        echo "--------------------------------------------------------------" . PHP_EOL;
        echo "--------------------------------------------------------------" . PHP_EOL;
        echo "INSERIDOS: " . $this->inseridos . PHP_EOL;
        echo "ATUALIZADOS: " . $this->atualizados . PHP_EOL;
        echo "ACERTADOS DEPARA: " . $this->acertados_depara . PHP_EOL;
        
        $time_end = microtime(true);
        $execution_time = ($time_end - $time_start);
        echo PHP_EOL . PHP_EOL . PHP_EOL;
        echo "----------------------------------" . PHP_EOL;
        echo "Total Execution Time: " . $execution_time . "s" . PHP_EOL . PHP_EOL . PHP_EOL;
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
        
        $fornecedorBonERP['codigo_ekt'] = $fornecedorEkt['CODIGO'];
        $fornecedorBonERP['inscricao_estadual'] = $fornecedorEkt['INSC'];
        $fornecedorBonERP['fone1'] = $fornecedorEkt['DDD_FONE'] . $fornecedorEkt['FONE'];
        $fornecedorBonERP['fone2'] = $fornecedorEkt['DDD_FAX'] . $fornecedorEkt['FAX'];
        $fornecedorBonERP['contato'] = $fornecedorEkt['CONTATO'];
        $fornecedorBonERP['representante'] = $fornecedorEkt['NOME_REPRES'];
        $fornecedorBonERP['representante_contato'] = $fornecedorEkt['DDD_REPRES'] . $fornecedorEkt['FONE_REPRES'];
        
        if ($atualizando) {
            $query = $this->dbbonerp->get_where("bon_pessoa", array(
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
            $this->dbbonerp->update('bon_pessoa', $pessoa, array(
                'id' => $pessoaId
            )) or $this->exit_db_error();
        } else {
            echo "Inserindo bon_pessoa... \n";
            $pessoa['inserted'] = date("Y-m-d H:i:s");
            $pessoa['estabelecimento_id'] = 1;
            $pessoa['user_inserted_id'] = 1;
            $pessoa['user_updated_id'] = 1;
            $this->dbbonerp->insert('bon_pessoa', $pessoa) or $this->exit_db_error();
            $pessoaId = $this->dbbonerp->insert_id();
        }
        
        $fornecedorBonERP['updated'] = date("Y-m-d H:i:s");
        $fornecedorBonERP['pessoa_id'] = $pessoaId;
        $fornecedorBonERP['fornecedor_tipo_id'] = 2;
        
        if ($atualizando) {
            echo "UPDATE est_fornecedor...\n";
            $this->dbbonerp->update('est_fornecedor', $fornecedorBonERP, array(
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
            $this->dbbonerp->insert('est_fornecedor', $fornecedorBonERP) or $this->exit_db_error();
            $fornecedorBonERP_id = $this->dbbonerp->insert_id();
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
            $query = $this->dbbonerp->query($sql, $params) or $this->exit_db_error();
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
            $this->dbbonerp->update('bon_endereco', $endereco, array(
                'id' => $enderecoId
            )) or $this->exit_db_error();
        } else {
            $endereco['version'] = 0;
            $endereco['inserted'] = date("Y-m-d H:i:s");
            $endereco['estabelecimento_id'] = 1;
            $endereco['user_inserted_id'] = 1;
            $endereco['user_updated_id'] = 1;
            
            $this->dbbonerp->insert('bon_endereco', $endereco) or $this->exit_db_error();
            
            $enderecoId = $this->dbbonerp->insert_id();
        }
        
        // many-to-many
        $query = $this->dbbonerp->get_where("est_fornecedor_enderecos", array(
            'est_fornecedor_id' => $fornecedorId,
            'bon_endereco_id' => $enderecoId
        )) or $this->exit_db_error();
        $r = $query->result_array();
        
        if (count($r) == 0) {
            $est_fornecedor_enderecos['est_fornecedor_id'] = $fornecedorId;
            $est_fornecedor_enderecos['bon_endereco_id'] = $enderecoId;
            $this->dbbonerp->insert('est_fornecedor_enderecos', $est_fornecedor_enderecos) or $this->exit_db_error();
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
        $corrigiu_algo_aqui = false;
        $fornecedorId = $fornecedorBonERP['id'];
        echo "LIDANDO COM 'depara' [$fornecedorId]... \n";
        
        $sql = "SELECT * FROM est_fornecedor_codektmesano WHERE fornecedor_id = ? AND mesano = ? AND codigo_ekt = ?";
        $params = array(
            $fornecedorId,
            $this->mesano,
            $fornecedorEkt['CODIGO']
        );
        $r = $this->dbbonerp->query($sql, $params)->result_array();
        
        // Se ainda não tem na est_fornecedor_codektmesano, insere...
        if (count($r) == 0) {
            $codektmesano['fornecedor_id'] = $fornecedorId;
            $codektmesano['mesano'] = $this->mesano;
            $codektmesano['codigo_ekt'] = $fornecedorEkt['CODIGO'];
            
            $this->dbbonerp->insert('est_fornecedor_codektmesano', $codektmesano) or $this->exit_db_error("Erro ao inserir na depara. fornecedor id [" . $fornecedorId . "]");
            $corrigiu_algo_aqui = true;
        }
        
        // verifica se o mesano é menor que o codigo_ekt_desde
        // se for, seta o codigo_ekt_desde pro mesano
        if (array_key_exists('codigo_ekt_desde', $fornecedorBonERP) and $fornecedorBonERP['codigo_ekt_desde']) {
            $dt_cod_ekt_desde = DateTime::createFromFormat('Y-m-d', $fornecedorBonERP['codigo_ekt_desde']);
            $dt_cod_ekt_desde->setTime(0, 0, 0, 0);
            if ($this->dtMesano < $dt_cod_ekt_desde) {
                $fornecedorBonERP['codigo_ekt_desde'] = $this->dtMesano->format('Y-m-d');
                $this->dbbonerp->update('est_fornecedor', $fornecedorBonERP, array(
                    'id' => $fornecedorBonERP['id']
                )) or $this->exit_db_error("Erro ao atualizar 'codigo_ekt_desde'");
                $corrigiu_algo_aqui = true;
            }
        }
        
        // verifica se o mesano é maior que o codigo_ekt_ate
        // se for, seta o codigo_ekt_ate pro mesano
        if (array_key_exists('codigo_ekt_ate', $fornecedorBonERP) and $fornecedorBonERP['codigo_ekt_ate']) {
            $dt_cod_ekt_ate = DateTime::createFromFormat('Y-m-d', $fornecedorBonERP['codigo_ekt_ate']);
            $dt_cod_ekt_ate->setTime(0, 0, 0, 0);
            if ($this->dtMesano > $dt_cod_ekt_ate) {
                $fornecedorBonERP['codigo_ekt_ate'] = $this->dtMesano->format('Y-m-d');
                $this->dbbonerp->update('est_fornecedor', $fornecedorBonERP, array(
                    'id' => $fornecedorBonERP['id']
                )) or $this->exit_db_error("Erro ao atualizar 'codigo_ekt_ate'");
                $corrigiu_algo_aqui = true;
            }
        }
        
        if ($corrigiu_algo_aqui) {
            $this->acertados_depara ++;
        }
    }

    public function findNovoCodigo($codigoEkt)
    {
        $query = $this->dbbonerp->get_where('est_fornecedor', array(
            'codigo' => $codigoEkt
        )) or $this->exit_db_error();
        if (count($query->result_array()) == 0) {
            return $codigoEkt;
        }
        
        $codigoStr = null;
        // codigo tem 9 dígitos (999999001)
        while (true) {
            $prefix = rand(1, 99999);
            $codigoStr = str_pad($prefix, 5, "9", STR_PAD_LEFT) . "0" . $codigoEkt;
            $query = $this->dbbonerp->get_where('est_fornecedor', array(
                'codigo' => $codigoStr
            )) or $this->exit_db_error("Erro ao buscar código randômico.");
            $existe = $query->result_array();
            if (count($existe) == 0) {
                return $codigoStr;
            }
        }
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
     */
    public function setar_codigos_ekt()
    {
        $time_start = microtime(true);
        $this->dbbonerp->trans_start();
        
        $sql = "SELECT * FROM ekt_fornecedor WHERE nome_fantasia IS NOT NULL AND trim(nome_fantasia) != '' ORDER BY id";
        $query = $this->dbekt->query($sql) or $this->exit_db_error();
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
            
            $query = $this->dbbonerp->query("SELECT * FROM vw_est_fornecedor WHERE REPLACE(nome_fantasia,' ','') LIKE ? AND tipo = 'ESTOQUE'", array(
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
        
        $this->dbbonerp->trans_complete();
        
        $time_end = microtime(true);
        $execution_time = ($time_end - $time_start);
        echo "\n\n\n\n----------------------------------\nTotal Execution Time: " . $execution_time . "s";
    }

    private function exit_db_error($msg = null)
    {
        echo str_pad("", 100, "*") . "\n";
        echo $msg ? $msg . PHP_EOL : '';
        echo "LAST QUERY: " . $this->dbbonerp->last_query() . PHP_EOL . PHP_EOL;
        print_r($this->dbbonerp->error()) . PHP_EOL . PHP_EOL;
        echo str_pad("", 100, "*") . PHP_EOL;
        exit();
    }
}
