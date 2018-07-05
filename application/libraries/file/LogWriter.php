<?php
require_once ("./application/libraries/phpmailer/class.phpmailer.php");

class LogWriter
{

    private $fh;

    private $fileName;

    private $logFile;

    private $debug;

    private $info;

    /**
     * INFO ou DEBUG.
     *
     * @var string
     */
    private $level = 'INFO';

    public function __construct($logPath, $prefix = null)
    {
        $this->openLog($logPath, $prefix);
    }

    public function setLevel($level)
    {
        $this->level = $level;
    }

    public function getLevel()
    {
        return $this->level;
    }

    private function openLog($logPath, $prefix = null)
    {
        $agora = new DateTime();
        
        $this->fileNameInfo = $prefix . $agora->format('Y-m-d_H-i-s') . ".info.log";
        $this->logFileInfo = $logPath . '/' . $this->fileNameInfo;
        $this->fhInfo = fopen($this->logFileInfo, 'a') or die("Impossível abrir arquivo de log: [" . $this->logFileInfo . "]" . PHP_EOL);
        
        $this->fileNameDebug = $prefix . $agora->format('Y-m-d_H-i-s') . ".debug.log";
        $this->logFileDebug = $logPath . '/' . $this->fileNameDebug;
        $this->fhDebug = fopen($this->logFileDebug, 'a') or die("Impossível abrir arquivo de log: [" . $this->logFileDebug . "]" . PHP_EOL);
    }

    public function debug($msg, $echoo = true)
    {
        $msg .= PHP_EOL;
        if ($echoo) {
            echo $msg;
        }        
        fwrite($this->fhDebug, $msg);
    }

    public function info($msg, $echoo = true)
    {
        $msg .= PHP_EOL;
        if ($echoo)
            echo $msg;
        fwrite($this->fhInfo, $msg);
        fwrite($this->fhDebug, $msg);
    }

    public function closeLog()
    {
        if (is_resource($this->fhInfo)) {
            fclose($this->fhInfo);
        }
        if (is_resource($this->fhDebug)) {
            fclose($this->fhDebug);
        }
    }

    public function sendMail($level = 'INFO')
    {
        $mail = new PHPMailer();
        $mail->IsSMTP(); // Ativar SMTP
        $mail->SMTPDebug = 0; // Debugar: 1 = erros e mensagens, 2 = mensagens apenas
        $mail->SMTPAuth = true; // Autenticação ativada
                                // $mail->SMTPSecure = 'ssl'; // SSL REQUERIDO pelo GMail
        $mail->Host = getenv('ekt2bonerp_mailer_host'); // SMTP utilizado
        $mail->Port = 587; // A porta 587 deverá estar aberta em seu servidor
        $mail->SMTPSecure = false; // Define se é utilizado SSL/TLS - Mantenha o valor "false"
        $mail->SMTPAutoTLS = false; // Define se, por padrão, será utilizado TLS - Mantenha o valor "false"
        $mail->Username = getenv('ekt2bonerp_mailer_user');
        $mail->Password = getenv('ekt2bonerp_mailer_pw');
        $mail->SetFrom(getenv('ekt2bonerp_mailer_user'), getenv('ekt2bonerp_mailer_user'));
        $mail->Subject = "RESULT: " . $this->fileNameInfo;
        $mail->Body = file_get_contents($level == 'INFO' ? $this->logFileInfo : $this->logFileDebug);
        $mail->AddAddress(getenv('ekt2bonerp_mailer_to'));
        if (! $mail->Send()) {
            $error = 'Mail error: ' . $mail->ErrorInfo;
            $this->info($error);
            return false;
        } else {
            $msg = 'E-mail enviado!';
            $this->info($msg);
            return true;
        }
    }
}