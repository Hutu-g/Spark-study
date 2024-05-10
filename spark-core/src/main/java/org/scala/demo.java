//public class demo {
//
//    public static void main(String[] args) {
//        int targetIndex = 20190324;
//        int[] sequence = new int[3]; // 存储当前需要求和的前 3 项
//
//        // 初始化数列前 3 项
//        sequence[0] = 1;
//        sequence[1] = 1;
//        sequence[2] = 1;
//
//        // 从第 4 项开始，模拟数列递推并仅保留最后 4 位
//        for (int i = 4; i <= targetIndex; i++) {
//            int nextTerm = (sequence[0] + sequence[1] + sequence[2]) % 10000;
//            // 移动数组元素以更新前 3 项
//            sequence[0] = sequence[1];
//            sequence[1] = sequence[2];
//            sequence[2] = nextTerm;
//        }
//
//        // 输出第 20190324 项的最后 4 位数字
//        System.out.printf("第 %d 项的最后 4 位数字为: %d%n", targetIndex, sequence[2]);
//    }
//}

public class demo {

    public static void main(String[] args) {
        //2000年的1月1日，是那一年的第1天。那么，2000年的5月4日，是那一年的第几天？
        System.out.println(dayOfYear(2000, 5, 4));
    }

    public static int dayOfYear(int year, int month, int day) {
        int[] days = {31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31};
        int sum = day;
        for (int i = 0; i < month - 1; i++) {
            sum += days[i];
        }
        if (month > 2 && isLeapYear(year)) {
            sum++;
        }
        return sum;
    }

    public static boolean isLeapYear(int year) {
        return (year % 4 == 0 && year % 100 != 0) || (year % 400 == 0);}


}
