<?php

/** FINANCIAL_MAX_ITERATIONS */
define('FINANCIAL_MAX_ITERATIONS', 128);
/** FINANCIAL_PRECISION */
define('FINANCIAL_PRECISION', 1.0e-08);


echo "<pre>";

// https://brownmath.com/bsci/loan.htm#Eq8
function rate($nprest, $vlrparc, $vp, $guess = 0.25) {
    $maxit = 100;
    $precision = 14;
    $guess = round($guess,$precision);
    for ($i=0 ; $i<$maxit ; $i++) {
        $divdnd = $vlrparc - ( $vlrparc * (pow(1 + $guess , -$nprest)) ) - ($vp * $guess);
        $divisor = $nprest * $vlrparc * pow(1 + $guess , (-$nprest - 1)) - $vp;
        $newguess = $guess - ( $divdnd / $divisor );
        $newguess = round($newguess, $precision);
        if ($newguess == $guess) {
            return $newguess;
        } else {
            $guess = $newguess;
        }
    }
    return null;
}

echo taxa(58, 8989.97, 260000);


exit;


$subject = "lkdjekljdewlPGTOCG\nVLR_ORIG=260000";


print_r($subject);

echo "\n\n---------------------------\n\n";

$pattern = '/(?P<label>VLR_ORIG={1})(?P<valor>\d+)/';
preg_match($pattern, substr($subject, 3), $matches);
print_r($matches);

print_r($matches['valor']);


echo "\n\n---------------------------\n\n";



$str = 'foobar: 2008';

preg_match('/(?P<name>\w+): (?P<digit>\d+)/', $str, $matches);

/* This also works in PHP 5.2.2 (PCRE 7.0) and later, however 
 * the above form is recommended for backwards compatibility */
// preg_match('/(?<name>\w+): (?<digit>\d+)/', $str, $matches);

print_r($matches);

?>