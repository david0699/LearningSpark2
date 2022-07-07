package exercises

import org.apache.log4j.{Level, Logger}
import java.lang.Math.round

object Shuntingyard {



  def main(args: Array[String]) {
    //To avoid WARN
    Logger.getLogger("org").setLevel(Level.ERROR)



    def calculate(firstOperation: String): Double = {
      var results: List[Double] = Nil
      var operators: List[String] = Nil
      var operation = firstOperation


      def precedence(operator: String) = operator match {
        case "+" | "-" => 0
        case "*" | "/" => 1
      }

      def execute(operator: String): Unit = {
        (results, operator) match {
          case (x :: y :: rest, "+") => results = (y + x) :: rest
          case (x :: y :: rest, "-") => results = (y - x) :: rest
          case (x :: y :: rest, "*") => results = (y * x) :: rest
          case (x :: y :: rest, "/") => results = (y / x) :: rest
          case (_, _) => throw new RuntimeException("Not enough arguments")
        }
      }

      val sol: Double = {
        var flag = true
        while (flag) {
          if (operation.contains("(")) {
            for (term <- "\\(([^()]+)\\)".r.findAllIn(operation)) {
              for (term1 <- "[1-9][0-9]*|[-+/*]".r.findAllIn(term)) {
                util.Try(term1.toInt) match {
                  case util.Success(number) => results ::= number
                  case _ =>
                    val (operatorsToExecute, rest) =
                      operators.span(op => precedence(op) >= precedence(term1))
                    operatorsToExecute foreach execute
                    operators = term1 :: rest
                }
              }
              operators foreach execute
              operation = operation.replace(term, results(0).toString)
              operators = Nil
            }
          } else {
            for (term1 <- "[-+]?[1-9][0-9]*|[-+/*]".r.findAllIn(operation)) {
              util.Try(term1.toInt) match {
                case util.Success(number) => results ::= number
                case _ =>
                  val (operatorsToExecute, rest) =
                    operators.span(op => precedence(op) >= precedence(term1))
                  operatorsToExecute foreach execute
                  operators = term1 :: rest
              }
            }
            operators foreach execute
            flag = false
            //results(0)
          }
        }
        results(0)
      }

   round(sol*100)/100.0

/*  results match {
    case res :: Nil => sol
    case _ => throw new RuntimeException("Too many arguments")
  }*/

}

val x = calculate("18 / (1 * (5+2-1))")
println(x)




}
}


