<?php


namespace CIBases\Libraries\Math;


class Financial {

    /**
     * Calcula a taxa de juros de um parcelamento de acordo com o Método de Newton.
     *
     * https://brownmath.com/bsci/loan.htm#Eq8
     *
     *
     * Newton’s Method
     *
     * Newton’s Method has the advantage that it’s very fast. More precisely, Newton’s Method finds the interest rate very quickly if it can find it at all. Newton’s Method can fail if your initial guess for the interest rate is too outlandish, but in the real world that’s not a problem because you usually have some idea of the actual interest rate.
     * Technology: I’ve created two ways for you to benefit from Newton’s Method without doing all the calculations by hand:
     * Download the Excel workbook that accompanies this page, or
     * Take advantage of a downloadable program for your TI-83/84 or TI-89/92.
     * You can read all about Newton’s Method at Mathworld. But briefly, Newton’s Method requires you to rewrite one of the equations involving interest rate so that the right-hand side is 0; the left-hand side is then called f(i). You then take the derivative, f′(i), and make an initial guess at the interest rate. From that guess i you form the next guess inew by the equation
     * inew = i − f(i)/f′(i)
     * and you repeat until there’s no change from one guess to the next. Usually this happens in less than half a dozen iterations.
     * P = iA/[1 − (1+i)^-N]Starting with equation 2 (shown at right), I subtract the right side from the left and clear fractions, then differentiate to get
     * f(i) = P − P (1+i)^-N − iA
     * f′(i) = N P (1+i)^(-N-1) − A
     * The equation for successive guesses in Newton’s Method is therefore
     * (8)i_new = i − [P − P (1+i)^-N − iA] / [ N P (1+i)^(-N-1) − A]
     * Example 6:
     * You’re thinking about leasing an $11,200 car, attracted by the advertisements of “no down payment”. The lease payment is $291 a month for four years. What is the effective interest rate you’d be paying on this lease?
     * Solution:  A = $11,200; P = $291; N = 4×12 = 48. Car loans in your area are quoted at around 12%, so start with i = 12%/year = .01/month. Using equation 8, the equation to get each guess from the previous guess is
     * inew = i − [291 − 291(1+i)^-48 − 11200i] / [ 48×291(1+i)^-49 − 11200]
     * The computed guesses are 0.0094295242, 0.0094008156, 0.0094007411, 0.0094007411. Newton’s Method takes only four iterations to reach an answer of i = 0.0094007411 per month. Check that by substituting that in equation 2—sure enough, we get P = $291.0000000. Therefore the annual rate is 12×0.94007411 = 11.28%.
     *
     *
     * @param type $nprest (número de prestações)
     * @param type $vlrparc (valor de cada parcela)
     * @param type $vp (valor presente)
     * @param type $guess (maior taxa possível para começar a adivinhar)
     * @return type
     */
    public function rate($nprest, $vlrparc, $vp, $guess = 0.25) {
        $maxit = 100;
        $precision = 14;
        $guess = round($guess, $precision);
        $vlrparc = abs($vlrparc);
        for ($i = 0; $i < $maxit; $i++) {
            $divdnd = $vlrparc - ($vlrparc * (pow(1 + $guess, -$nprest))) - ($vp * $guess);
            $divisor = $nprest * $vlrparc * pow(1 + $guess, (-$nprest - 1)) - $vp;
            $newguess = $guess - ($divdnd / $divisor);
            $newguess = round($newguess, $precision);
            if ($newguess == $guess) {
                return $newguess;
            } else {
                $guess = $newguess;
            }
        }
        return null;
    }

    /**
     * Calcula o valor da dívida atualizada.
     *
     * @param type $vlrparc (valor da parcela)
     * @param type $taxa (taxa de juros)
     * @param type $nparcrest (número de parcelas restantes)
     * @return type
     */
    public function atualiza_divida($vlrparc, $taxa, $nparcrest) {
        return $vlrparc * ((pow(1 + $taxa, $nparcrest) - 1) / ($taxa * pow(1 + $taxa, $nparcrest)));
    }

}


