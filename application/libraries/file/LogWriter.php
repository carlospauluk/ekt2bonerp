<?php
require_once ("./application/libraries/phpmailer/class.phpmailer.php");

class LogWriter
{

    private $fh;

    private $fileName;

    private $logFile;

    public function __construct($logPath, $prefix = null)
    {
        $this->openLog($logPath, $prefix);
    }

    private function openLog($logPath, $prefix = null)
    {
        $agora = new DateTime();
        $this->fileName = $prefix . $agora->format('Y-m-d_H-i-s') . ".log";
        $this->logFile = $logPath . '/' . $this->fileName;
        $this->fh = fopen($this->logFile, 'a') or die("Impossível abrir arquivo de log: [" . $this->logFile . "]" . PHP_EOL);
    }

    public function writeLog($msg, $echoo = true)
    {
        if ($echoo)
            echo $msg;
        fwrite($this->fh, $msg);
    }

    public function closeLog()
    {
        if (is_resource($this->fh)) {
            fclose($this->fh);
        }
    }

    public function sendMail()
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
        $mail->Subject = "RESULT: " . $this->fileName;
        $mail->Body = file_get_contents($this->logFile);
        $mail->AddAddress(getenv('ekt2bonerp_mailer_to'));
        if (! $mail->Send()) {
            $error = 'Mail error: ' . $mail->ErrorInfo;
            return false;
        } else {
            $error = 'Mensagem enviada!';
            return true;
        }
    }
}