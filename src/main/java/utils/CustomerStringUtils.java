package utils;

/**
 * @author shaco
 * @create 2023-03-23 17:04
 * @desc 用户自定义字符串方法
 */
public class CustomerStringUtils {
    /**
     * @param str     需要删除字符（串）的字符串
     * @param strChar 需要删除的字符串
     * @return 删除了首尾指定字符的字符串
     */
    public static String trim(String str, String strChar) {
        if (str == null) return null;
        if (strChar == null) return str;
        if (str == "") return "";
        if (strChar == "") return str;
        if (str.equals(strChar)) return "";

        if (str.length() < strChar.length()) return str;
        if (str.length() == strChar.length()) return str;

        int length = strChar.length();
        String leftSubString = str.substring(0, length);
        String rightSubString = str.substring(str.length() - length, str.length());
        if (leftSubString.equals(strChar) && rightSubString.equals(strChar))
            return str.substring(length, str.length() - length);

        if (leftSubString.equals(strChar) && !rightSubString.equals(strChar))
            return str.substring(length, str.length());

        if (!leftSubString.equals(strChar) && rightSubString.equals(strChar))
            return str.substring(0, str.length() - length);

        if (!leftSubString.equals(strChar) && !rightSubString.equals(strChar))
            return str;

        return null;
    }

    /**
     * 在指定的字符之前插入字符串
     *
     * @param str        需要插入字符的字符串
     * @param strChar    指定的字符
     * @param insertChar 指定的插入的字符
     * @return
     */
    public static String addChar(String str, String strChar, String insertChar) {
        if (str == null || str == "") return str;
        StringBuilder stringBuilder = new StringBuilder();

        if (str.substring(0, 1).equals(strChar)) stringBuilder.append(insertChar + strChar);


        return null;
    }

    public static void main(String[] args) {
        String s = "abcdefg";
        String fg = CustomerStringUtils.trim(s, "fg");
        System.out.println(fg);

    }
}
