
* file to be restarted from [case].g00 created by createmodel.gms 
* exports can_share.gdx from which gdxdump calls are made

parameter share_imports, share_exports, total_imports, total_exports;


share_imports(r,'wecc',t)$r_interconnect(r,'western') = can_imports(r,t) / sum(rr$r_interconnect(rr,'western'),can_imports(rr,t));
share_imports(r,'east',t)$r_interconnect(r,'eastern') = can_imports(r,t) / sum(rr$r_interconnect(rr,'eastern'),can_imports(rr,t));

share_exports(r,'wecc',t)$r_interconnect(r,'western') = can_exports(r,t) / sum(rr$r_interconnect(rr,'western'),can_exports(rr,t));
share_exports(r,'east',t)$r_interconnect(r,'eastern') = can_exports(r,t) / sum(rr$r_interconnect(rr,'eastern'),can_exports(rr,t));

total_imports('wecc',t) = sum(r$r_interconnect(r,'western'),can_imports(r,t));
total_imports('east',t) = sum(r$r_interconnect(r,'eastern'),can_imports(r,t));

total_exports('west',t) = sum(r$r_interconnect(r,'western'),can_exports(r,t));
total_exports('east',t) = sum(r$r_interconnect(r,'eastern'),can_exports(r,t));


execute_unload 'can_share.gdx' share_imports, share_exports, total_imports, total_exports;;
