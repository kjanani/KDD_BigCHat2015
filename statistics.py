# K. Janani
# 6.25.2015

import os, sys
import numpy, scipy
import scipy.stats

if __name__ == '__main__':
#def myCode():

    R = numpy.loadtxt('../rumour.txt');
    T = numpy.loadtxt('../truth.txt');

    fid = open('statistical_test_results.txt','w');
    cols = map(lambda x: x.strip(), open('../for_stats_test.txt','r').readlines());
   
    fid.write('feature;truth;rumour;hyp;p\n'); 
    for i in range(len(cols)-1):
        print cols[i]
        stat_result = scipy.stats.ttest_ind(T[:,i],R[:,i],equal_var=False);
        if(stat_result[1] < 0.002):
            hyp = 1;
        else:
            hyp = 0;
        myStr = str(cols[i]) + ';' + str(numpy.average(T[:,i])) + ';' + str(numpy.average(R[:,i])) + ';' + str(stat_result[1]) + ';' + str(hyp)+'\n';
        fid.write(myStr);

    fid.close()
