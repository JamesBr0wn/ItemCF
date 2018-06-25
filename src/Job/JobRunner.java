package Job;

import Step1.MapReduce1;
import Step2.MapReduce2;
import Step3.MapReduce3;
import Step4.MapReduce4;
import Step5.MapReduce5;
import Step6.MapReduce6;

public class JobRunner {
    public static void main(String[] args){
        int state = 0;
        state = new MapReduce1().run();
        if(state == 0){
            System.out.println("Step1 Successful!");
        }else{
            System.out.println("Step1 Fail!");
            return;
        }

        state = new MapReduce2().run();
        if(state == 0){
            System.out.println("Step2 Successful!");
        }else{
            System.out.println("Step2 Fail!");
            return;
        }

        state = new MapReduce3().run();
        if(state == 0){
            System.out.println("Step3 Successful!");
        }else{
            System.out.println("Step3 Fail!");
            return;
        }

        state = new MapReduce4().run();
        if(state == 0){
            System.out.println("Step4 Successful!");
        }else{
            System.out.println("Step4 Fail!");
            return;
        }

        state = new MapReduce5().run();
        if(state == 0){
            System.out.println("Step5 Successful!");
        }else{
            System.out.println("Step5 Fail!");
            return;
        }
        state = new MapReduce6().run();
        if(state == 0){
            System.out.println("Step6 Successful!");
        }else{
            System.out.println("Step6 Fail!");
            return;
        }
        System.out.println("Program execute Successful!");
    }
}
