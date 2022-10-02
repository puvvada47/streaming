package org.example;

public class Lastrepe {
    public static void main(String[] args) {

       int [] a={1,5,5,2,8,9,11,12,12,14,14,18,21};

        for(int i=a.length-1;i>0;i--){
            int count = 1;
                for (int j = i - 1; j > 0; j--) {
                    if (a[j] > 0) {
                        if (a[i] == a[j]) {
                            count += 1;
                            a[j] = -1;
                        }

                    }
                }


            if(count>1){
                System.out.println("non repeated number: "+count+" "+a[i]);
                break;
            }
        }





    }
}
