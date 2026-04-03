# [mycase] needs to be replaced with appropriate case name
# e.g. mb_ref_seq - this is the file created from a call to createmodel.gms
gams output_canshares.gms r=[mycase]

#dump those files to csv
gdxdump can_share.gdx symb=share_imports format=csv > share_imports.csv
gdxdump can_share.gdx symb=share_exports format=csv > share_exports.csv
gdxdump can_share.gdx symb=total_imports format=csv > total_imports.csv
gdxdump can_share.gdx symb=total_exports format=csv > total_exports.csv

#following call will load in these gdxdump'd CSVs
#as well as trade.csv to to output two files:
#net_can_trade_h17.csv 
#can_trade_8760.csv
python CAN_to8760.py
