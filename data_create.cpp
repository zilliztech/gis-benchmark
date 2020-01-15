#include <iostream>
#include <fstream>
#include <stdlib.h>
using namespace std;

const int count = 100000000;
double jingdu(){
    int number = rand();
    double result = (double)number /((double)RAND_MAX/179);
    int biaozhi=rand();
    if(biaozhi<RAND_MAX/2)
        result = result*(-1);
    return result;
}

double weidu(){
    int number = rand();
    double result = (double)number /((double)RAND_MAX/89);
    int biaozhi=rand();
    if(biaozhi<RAND_MAX/2)
        result = result*(-1);
    return result;
}


class polygon{
    public:
    polygon(){
        jingdu1=jingdu();
        weidu1=weidu();
        jingdu2=jingdu1*(1-1.0/100);
        weidu2=weidu1;
        jingdu3=jingdu2;
        weidu3=weidu1*(1-1.0/100);
        jingdu4=jingdu1;
        weidu4=weidu3;
        jingdu5=jingdu1;
        weidu5=weidu1;
    }
    double jingdu1,jingdu2,jingdu3,jingdu4,jingdu5;
    double weidu1,weidu2,weidu3,weidu4,weidu5;
    
};

struct point{
    public:
    point(){
        jingdu1=jingdu();
        weidu1=weidu();
    }
    double jingdu1;
    double weidu1;
};

int main(){
    ofstream out("ceshi_data.csv");
    if (out.is_open()) 
    {
        for(int i=1; i<=count;i++){
            double jingdu1=jingdu();
            double weidu1=weidu();
            double jingdu2=jingdu();
            double weidu2=weidu();
            polygon* polygon1=new polygon();
            polygon* polygon2=new polygon();
            point* point1=new point();
            point* point2=new point();
            out<<jingdu1<<";"<<weidu1<<";"<<jingdu2<<";"<<weidu2<<";POLYGON(("
            <<polygon1->jingdu1<<" "<<polygon1->weidu1<<","<<polygon1->jingdu2<<" "<<polygon1->weidu2<<","<<polygon1->jingdu3<<" "<<polygon1->weidu3<<","
            <<polygon1->jingdu4<<" "<<polygon1->weidu4<<","<<polygon1->jingdu5<<" "<<polygon1->weidu5<<"));"
            <<"POLYGON(("<<polygon2->jingdu1<<" "<<polygon2->weidu1<<","<<polygon2->jingdu2<<" "<<polygon2->weidu2<<","<<polygon2->jingdu3
            <<" "<<polygon2->weidu3<<","<<polygon2->jingdu4<<" "<<polygon2->weidu4<<","<<polygon2->jingdu5<<" "<<polygon2->weidu5<<"));" 
            <<"POINT("<<point1->jingdu1<<" "<<point1->weidu1<<");"
            <<"POINT("<<point2->jingdu1<<" "<<point2->weidu1<<")"<<endl;
        }
        out.close();
    }
    return 0;
}