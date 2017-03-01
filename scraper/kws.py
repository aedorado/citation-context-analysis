from DB import DB

db = DB()
all_papers = db.select_all('metadata')

kw_dict = {}
for paper in all_papers:
    kws = paper[3]
    kws = kws.split(';')
    for kw in kws:
        if kw in kw_dict.keys():
            kw_dict[kw] = kw_dict[kw] + 1
        else:
            kw_dict[kw] = 1
            
for key in kw_dict.keys():
    if kw_dict[key] >= 10:
        print key, ' -> ', kw_dict[key]