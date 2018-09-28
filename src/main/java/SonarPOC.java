public class SonarPOC {

    public static int addition(int a, int b){
        return a+b;
    }

    public static void main(String args[]){
        int a = 10;
        int b = 20;
        System.out.println("Sum of " + a + " and " + b + " is : " + addition(a,b));
    }
}
