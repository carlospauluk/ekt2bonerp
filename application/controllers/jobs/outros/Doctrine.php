<?php

/**
 * Gerador de código fonte para entidades do Doctrine a partir das tabelas da crosier.
 * 
 * @author Carlos Eduardo Pauluk
 *
 */
class Doctrine extends CI_Controller
{

    public function __construct()
    {
        parent::__construct();
        ini_set('max_execution_time', 0);
        ini_set('memory_limit', '2048M');
        
        $this->dbcrosier = $this->load->database('crosier', TRUE);
        
        $this->load->library('datetime_library');
        error_reporting(E_ALL);
        ini_set('display_errors', 1);
        
        $this->load->model('est/produto_model');
        $this->agora = new DateTime();
    }

    public function generateDoctrineEntity($table)
    {
        $r = $this->dbcrosier->query("DESC " . $table)->result_array();
        $i = 0;
        $g = 0;
        $nGerados = "";
        
        echo "<textarea style='width: 100%; height: 100%'>";
        
        $this->genHead($table);
        
        foreach ($r as $l) {
            if (in_array($l['Field'], array(
                'inserted',
                'updated',
                'user_inserted_id',
                'user_updated_id',
                'estabelecimento_id',
                'version'
            ))) {
                continue;
            }
            
            $i ++;
            // print_r($l);
            
            $type = strpos($l['Type'], '(') ? substr($l['Type'], 0, strpos($l['Type'], '(')) : $l['Type'];
            // echo $type . PHP_EOL;
            
            if ($l['Field'] == 'id') {
                $this->genId();
                $g ++;
                continue;
            }
            
            if (substr($l['Field'], - 3) == '_id') {
                if ($type != 'bigint') {
                    echo "Não é bigint????" . PHP_EOL;
                }
                $this->genManyToOne($l);
                $g ++;
                continue;
            }
            
            switch ($type) {
                case 'varchar':
                    $this->genVarchar($l);
                    $g ++;
                    break;
                case 'int':
                case 'bigint':
                    $this->genIntBigint($l);
                    $g ++;
                    break;
                case 'datetime':
                case 'date':
                    $this->genDateTime($l);
                    $g ++;
                    break;
                case 'decimal':
                    $this->genDecimal($l);
                    $g ++;
                    break;
                case 'bit':
                    $this->genBoolean($l);
                    $g ++;
                    break;
                default:
                    $nGerados .= $l['Field'] . ",";
            }
        }
        
        $this->genConstruct();
        
        echo "TOTAL DE CAMPOS: " . $i . PHP_EOL;
        echo "TOTAL GERADOS: " . $g . PHP_EOL;
        echo "NÃO GERADOS: " . $nGerados . PHP_EOL;
    }

    public function genHead($table)
    {
        $entity = $this->convertToPojoName(ucfirst(substr($table, strpos($table,'_')+1)));
        
        
        // @formatter:off
        echo    "<?php" . PHP_EOL . 
                "namespace App\Entity\?????????????;" . PHP_EOL . PHP_EOL .
                "use App\Entity\Base\EntityId;" . PHP_EOL .
                "use Doctrine\ORM\Mapping as ORM;" . PHP_EOL .
                "use Symfony\Component\Validator\Constraints as Assert;"  . PHP_EOL . PHP_EOL .
                "/**" . PHP_EOL .
                "*" . PHP_EOL .
                "* @ORM\Entity(repositoryClass=\"App\Repository\????????\\" . $entity . "Repository\")" . PHP_EOL .
                "* @ORM\Table(name=\"" . $table . "\")" . PHP_EOL . 
                "*/" . PHP_EOL . 
                "class " . $entity . " extends EntityId" . PHP_EOL . "{";
        // @formatter:on
    }
    
    public function genConstruct() {
        // @formatter:off
        echo    "public function __construct()" . PHP_EOL . 
                "{" . PHP_EOL .
                "ORM\Annotation::class;" . PHP_EOL .
                "Assert\All::class;" . PHP_EOL .
                "}" . PHP_EOL;
        // @formatter:on
        
        echo PHP_EOL . PHP_EOL;
    }

    public function genId()
    {
        // @formatter:off
        echo "
        /**
         *
         * @ORM\Id()
         * @ORM\GeneratedValue()
         * @ORM\Column(type=\"bigint\")
         */
        private \$id;";
        // @formatter:on
        
        echo PHP_EOL . PHP_EOL;
    }

    public function genVarchar($l)
    {
        $campo = $l['Field'];
        $pojoName = $this->convertToPojoName($campo);
        $podeSerNull = $l['Null'] == 'YES' ? true : false;
        $r1 = '[^(]*';
        $r2 = '\(';
        $r3 = '(\d+)';
        $r4 = '\)';
        preg_match('/' . $r1 . $r2 . $r3 . $r4 . '/', $l['Type'], $matches);
        $size = $matches[1];
        
        // echo "CAMPO: " . $campo . ". NULL: " . $null . ". SIZE: " . $size . "." . PHP_EOL;
        
        // @formatter:off
        echo    "/**" . PHP_EOL .
                "*" . PHP_EOL .
                "* @ORM\Column(name=\"" . $campo . "\", type=\"string\", nullable=" . ($podeSerNull ? "true" : "false") . ", length=" . $size . ")" . PHP_EOL .
                (!$podeSerNull ? "* @Assert\NotBlank(message=\"O campo '" . $campo . "' deve ser informado\")" . PHP_EOL : "") .
                "*/" . PHP_EOL .
                "private \$" . $pojoName . ";";
        // @formatter:on
        
        echo PHP_EOL . PHP_EOL;
    }

    public function genIntBigInt($l)
    {
        $campo = $l['Field'];
        $type = strpos($l['Type'], '(') ? substr($l['Type'], 0, strpos($l['Type'], '(')) : $l['Type'];
        $tipo = $type == 'int' ? 'integer' : 'bigint';
        $pojoName = $this->convertToPojoName($campo);
        $podeSerNull = $l['Null'] == 'YES' ? true : false;
        
        // @formatter:off
        echo    "/**" . PHP_EOL . 
                "*" . PHP_EOL . 
                "* @ORM\Column(name=\"" . $campo . "\", type=\"" . $tipo . "\", nullable=" . ($podeSerNull ? "true" : "false") . ")" . PHP_EOL . 
                (!$podeSerNull ? "* @Assert\NotBlank(message=\"O campo '" . $campo . "' deve ser informado\")" . PHP_EOL : "") . 
                "* @Assert\Range(min = 0)" . PHP_EOL . 
                "*/" . PHP_EOL . 
                "private \$" . $pojoName . ";";
        // @formatter:ON
        echo PHP_EOL . PHP_EOL;
    }
    
    public function genDecimal($l)
    {
        $campo = $l['Field'];
        $pojoName = $this->convertToPojoName($campo);
        $podeSerNull = $l['Null'] == 'YES' ? true : false;
        
        
        // @formatter:off
        echo    "/**" . PHP_EOL . 
                "*" . PHP_EOL . 
                "* @ORM\Column(name=\"" . $campo . "\", type=\"decimal\", nullable=" . ($podeSerNull ? "true" : "false") . ", precision=15, scale=2)" . PHP_EOL . 
                (!$podeSerNull ? "* @Assert\NotNull(message=\"O campo '" . $campo . "' deve ser informado\")" . PHP_EOL : "") . 
                "* @Assert\Type(\"numeric\", message=\"O campo '" . $campo . "' deve ser numérico\")" . PHP_EOL .
                "*/" . PHP_EOL . 
                "private \$" . $pojoName . ";";
        // @formatter:ON
        echo PHP_EOL . PHP_EOL;
    }
    
    public function genBoolean($l)
    {
        $campo = $l['Field'];
        $pojoName = $this->convertToPojoName($campo);
        $podeSerNull = $l['Null'] == 'YES' ? true : false;
        
        
        // @formatter:off
        echo    "/**" . PHP_EOL . 
                "*" . PHP_EOL . 
                "* @ORM\Column(name=\"" . $campo . "\", type=\"boolean\", nullable=" . ($podeSerNull ? "true" : "false") . ")" . PHP_EOL . 
                (!$podeSerNull ? "* @Assert\NotNull(message=\"O campo '" . $campo . "' deve ser informado\")" . PHP_EOL : "") . 
                "*/" . PHP_EOL . 
                "private \$" . $pojoName . ";";
        // @formatter:ON
        echo PHP_EOL . PHP_EOL;
        
    }

    public function genDateTime($l)
    {
        $campo = $l['Field'];
        
        $pojoName = $this->convertToPojoName($campo);
        $podeSerNull = $l['Null'] == 'YES' ? true : false;
        
        // @formatter:off
        echo    "/**" . PHP_EOL .
                "* " . PHP_EOL .
                "* @ORM\Column(name=\"" . $campo . "\", type=\"" . $l['Type'] . "\", nullable=" . ($podeSerNull ? "true" : "false") . ")" . PHP_EOL .
                ($podeSerNull ? "* @Assert\NotNull(message=\"O campo '" . $campo . "' deve ser informado\")" . PHP_EOL : "") .
                "* @Assert\Type(\"\\DateTime\", message=\"O campo '" . $campo . "' deve ser do tipo data/hora\")" . PHP_EOL .
                "*/" . PHP_EOL .
                "private \$" . $pojoName . ";";
        // @formatter:on
        
        echo PHP_EOL . PHP_EOL;
    }

    public function genManyToOne($l)
    {
        $target = substr($l['Field'], 0, - 3);
        $podeSerNull = $l['Null'] == 'YES' ? true : false;
        $pojoName = $this->convertToPojoName($target);
        
        // @formatter:off
        echo    "/**" . PHP_EOL . 
                "*" . PHP_EOL . 
                "* @ORM\ManyToOne(targetEntity=\"App\Entity\????????????????\\" . ucfirst($pojoName) . "\")" . PHP_EOL . 
                "* @ORM\JoinColumn(name=\"" . $l['Field'] . "\", nullable=" . ($podeSerNull ? "true" : "false") . ")" . PHP_EOL . 
                ($podeSerNull ? "* @Assert\NotNull(message=\"O campo '" . ucfirst($target) . "' deve ser informado\")" . PHP_EOL : "") . 
                "*" . PHP_EOL . "* @var $" . $pojoName . " " . ucfirst($pojoName) . PHP_EOL . 
                "*/" . PHP_EOL . 
                "private $" . $pojoName . ";";
        
        echo PHP_EOL . PHP_EOL;
    }

    public function convertToPojoName($field)
    {
        $corr = $field;
        while (true) {
            if (strpos($corr, "_")) {
                $corr = substr($corr, 0, strpos($corr, "_")) . strtoupper($corr[strpos($corr, "_") + 1]) . substr($corr, strpos($corr, "_") + 2);
            } else {
                return $corr;
            }
        }
    }
}
