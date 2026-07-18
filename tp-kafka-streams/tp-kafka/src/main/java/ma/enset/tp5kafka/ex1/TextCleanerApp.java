package ma.enset.tp5kafka.ex1;

public class TextCleanerApp {

  /**
   * Nettoie le message :
   * - supprime les espaces au début et à la fin
   * - remplace les espaces multiples par un seul
   * - convertit en majuscules
   */
  public static String clean(String message) {

    if (message == null) {
      return "";
    }

    return message
            .trim()
            .replaceAll("\\s+", " ")
            .toUpperCase();
  }

  /**
   * Vérifie si le message est valide
   */
  public static boolean isValid(String message) {

    if (message == null || message.isBlank()) {
      return false;
    }

    if (message.length() > 100) {
      return false;
    }

    if (message.contains("HACK")
            || message.contains("SPAM")
            || message.contains("XXX")) {
      return false;
    }

    return true;
  }
}