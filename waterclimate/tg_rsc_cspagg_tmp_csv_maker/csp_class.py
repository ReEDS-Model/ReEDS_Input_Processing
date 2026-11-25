"""
Run this gams code, and extract csp_class variable to a file as Excel 2007, create a csv file just keeping its column names. 
The csp_class.csv should serve as input for this Python script.

set tg_rsc_cspagg_temp(i,ii) ;

set class_idx / 1*12 /,
csp_class(i,class_idx) 
    /
    (csp1_1, csp2_1, csp3_1, csp4_1).1,
    (csp1_2, csp2_2, csp3_2, csp4_2).2,
    (csp1_3, csp2_3, csp3_3, csp4_3).3,
    (csp1_4, csp2_4, csp3_4, csp4_4).4,
    (csp1_5, csp2_5, csp3_5, csp4_5).5,
    (csp1_6, csp2_6, csp3_6, csp4_6).6,
    (csp1_7, csp2_7, csp3_7, csp4_7).7,
    (csp1_8, csp2_8, csp3_8, csp4_8).8,
    (csp1_9, csp2_9, csp3_9, csp4_9).9,
    (csp1_10, csp2_10, csp3_10, csp4_10).10,
    (csp1_11, csp2_11, csp3_11, csp4_11).11,
    (csp1_12, csp2_12, csp3_12, csp4_12).12
    / 
;

csp_class(i,class_idx)$[i_water_cooling(i)$csp1(i)] = sum(ii$ctt_i_ii(i,ii),csp_class(ii,class_idx)) ;
csp_class(i,class_idx)$[i_water_cooling(i)$csp2(i)] = sum(ii$ctt_i_ii(i,ii),csp_class(ii,class_idx)) ;
csp_class(i,class_idx)$[i_water_cooling(i)$csp3(i)] = sum(ii$ctt_i_ii(i,ii),csp_class(ii,class_idx)) ;
csp_class(i,class_idx)$[i_water_cooling(i)$csp4(i)] = sum(ii$ctt_i_ii(i,ii),csp_class(ii,class_idx)) ;

"""

import pandas as pd

csp_class = pd.read_csv('csp_class.csv', sep=',')
myfile = open('tg_rsc_cspagg_tmp.csv', 'w')

myfile.write('*i,ii\n')
for i, v in zip(csp_class['i'], csp_class['class_idx']):
    for ii,vv in zip(csp_class['i'], csp_class['class_idx']):
        if v == vv and ('csp1' in i):
            myfile.write(f'{i}, {ii}\n')
myfile.close()
