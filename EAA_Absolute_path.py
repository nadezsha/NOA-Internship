from re import L
import pandas as pd
import numpy as np
import datetime 
import os
import glob


path = 'C:/Users/nadia/OneDrive/Desktop/pirpython'
extension = 'csv'
os.chdir(path)
files_lst = glob.glob('*.{}'.format(extension))

for fl in files_lst:
    if fl[-9:] != 'utput.csv':
        namee = str(fl[:-4])
        print(fl, namee)
        lines_of_new_file = []
        data = pd.read_csv(fl)
        data['DATETIME']=pd.to_datetime(data['DATETIME'])

        print("DATASET")
        print(data, '\n')


        # In[213]:


        # adding columns for later

        data['date'] = data['DATETIME'].dt.date
        data['O3rcount'] = data['O3'].rolling(8, min_periods=8).count()
        data['COrcount'] = data['CO'].rolling(8, min_periods=8).count()

        #for i in data['O3rcount'].dropna():
        #   if data['O3rcount'][i] < 8:
        #      data['O3'][i] = ''   

        filter1 = data["O3rcount"] == 7
        filter2 = data["O3rcount"] == 8
        #data["O3"] = np.where(filter1 & filter2 , data['O3'], 0)

        fil1 = data['O3rcount'] < 6
        fil2 = data['O3rcount'].isnull()
        data['O3'].mask(fil1 | fil2, 0)

        data['O3roll'] = data['O3'].rolling(8).mean()
        data['COroll'] = data['CO'].rolling(8).mean()

        print("ADDED COLUMNS")
        print(data.head(350),'\n')


        # In[100]:


        #second dataframe for calculations

        grouped_single = data.groupby('date').agg({'SO2': ['count','mean'], 'PM10': ['count','mean'], 'PM2.5': ['count','mean'], 'O3roll': ['count','max'] , 'COroll': ['count','max']})
        grouped_single.replace('', np.nan).count()
        grouped_single.head(10)


        # # C6H6

        # In[101]:



        # print("ΒΕΝΖΟΛΙΟ", '\n')
        lines_of_new_file.append("ΒΕΝΖΟΛΙΟ")
        lines_of_new_file.append(" ")


        # In[102]:


        #Μέτρηση έγκυρων τιμών και έλεγχος πληρότητας

        data.replace('', np.nan).count()
        c = data['C6H6'].count()
        # print("Οι συνολικές ωριαίες τιμές είναι", data.shape[0] )
        lines_of_new_file.append(f"Οι συνολικές ωριαίες τιμές είναι: {data.shape[0]} ")
        # print("Οι έγκυρες ωριαίες τιμές είναι", c)
        lines_of_new_file.append(f"Οι έγκυρες ωριαίες τιμές είναι: {c}")
        if c > (0.9*data.shape[0]):
            lines_of_new_file.append("Ικανοποιείται ο έλεγχος πληρότητας (90% των ωριαίων τιμών)")
            # print("Ικανοποιείται ο έλεγχος πληρότητας (90% των ωριαίων τιμών)")
        else:
            lines_of_new_file.append("Δεν ικανοποιείται ο έλεγχος πληρότητας(90% των ωριαίων τιμών)")
            #print("Δεν ικανοποιείται ο έλεγχος πληρότητας(90% των ωριαίων τιμών)")


        # In[103]:


        # Έλεγχος οριακής τιμής στο ημερολογιακό έτος (5 μg/m3)

        #print("Μέση παρατηρούμενη ετήσια τιμή:", round(data['C6H6'].mean(), 2), "μg/m3")
        rdch6 = round(data['C6H6'].mean(), 2)
        lines_of_new_file.append(f"Μέση παρατηρούμενη ετήσια τιμή: {rdch6} μg/m3 ")
        if data['C6H6'].mean() > 5:
            lines_of_new_file.append("Γίνεται υπέρβαση του ορίου (5 μg/m3)")
            #print("Γίνεται υπέρβαση του ορίου (5 μg/m3)")
        else:
            lines_of_new_file.append("Δεν παρατηρείται υπέρβαση του ορίου (5 μg/m3)")
            #print("Δεν παρατηρείται υπέρβαση του ορίου (5 μg/m3)", '\n')
            


        # # NO2

        # In[104]:

        lines_of_new_file.append(" ")
        # print("NO2", '\n')
        lines_of_new_file.append("NO2")
        lines_of_new_file.append(" ")


        # In[105]:


        #Μέτρηση έγκυρων τιμών και έλεγχος πληρότητας (ωριαίο)
        dno2 = data['NO2'].count()
        lines_of_new_file.append(f"Οι συνολικές τιμές είναι {data.shape[0]}")
        lines_of_new_file.append(f"Οι έγκυρες τιμές είναι {dno2}")
        #print("Οι συνολικές τιμές είναι", data.shape[0] )
        #print("Οι έγκυρες τιμές είναι", data['NO2'].count())
        if data['NO2'].count() > (0.9*data.shape[0]):
            lines_of_new_file.append("Ικανοποιείται ο έλεγχος πληρότητας (90% των ωριαίων τιμών)")
            #print("Ικανοποιείται ο έλεγχος πληρότητας (90% των ωριαίων τιμών)")
        else:
            lines_of_new_file.append("Δεν ικανοποιείται ο έλεγχος πληρότητας (90% των ωριαίων τιμών)")
            #print("Δεν ικανοποιείται ο έλεγχος πληρότητας (90% των ωριαίων τιμών)")


        # In[217]:


        # Ωριαία οριακή τιμή (200μg/m3 έως 18 φορές τον χρόνο)

        if data.loc[data.NO2 > 200, 'NO2'].count() > 18 :
            lines_of_new_file.append("Γίνεται υπέρβαση του ορίου (200μg/m3 έως 18 φορές τον χρόνο)")
            print("Γίνεται υπέρβαση του ορίου (200μg/m3 έως 18 φορές τον χρόνο)")
        else:
            dnoo22loc = data.loc[data.NO2 > 200, 'NO2'].count()
            lines_of_new_file.append(f"Δεν γίνεται υπέρβαση του ορίου στην ωριαία οριακή τιμή (200μg/m3) για πάνω από 18 φορές το έτος. Aριθμός υπερβάσεων: {dnoo22loc}")
            #print("Δεν γίνεται υπέρβαση του ορίου στην ωριαία οριακή τιμή (200μg/m3) για πάνω από 18 φορές το έτος. Aριθμός υπερβάσεων:", data.loc[data.NO2 > 200, 'NO2'].count())


        # In[214]:


        # Οριακή τιμή στο ημερολογ. έτος = 40 μg/m3

        if data['NO2'].mean() > 40:
            dno2mean = round(data['NO2'].mean(), 1)
            lines_of_new_file.append(f"Γίνεται υπέρβαση του ορίου (40 μg/m3): {dno2mean} μg/m3")
        else:
            lines_of_new_file.append("Δεν παρατηρείται υπέρβαση του ορίου (40 μg/m3)")
            # print("Δεν παρατηρείται υπέρβαση του ορίου (40 μg/m3)", '\n')


        # # SO2

        # In[108]:

        lines_of_new_file.append(" ")
        # print("SO2",'\n')
        lines_of_new_file.append("SO2")


        # In[109]:


        #Μέτρηση έγκυρων τιμών και έλεγχος πληρότητας (ωριαίο)
        so2c = data['SO2'].count()
        lines_of_new_file.append(f"Οι συνολικές ωριαίες τιμές είναι {data.shape[0]}")
        lines_of_new_file.append(f"Οι έγκυρες ωριαίες τιμές είναι {so2c}")
        #print("Οι συνολικές ωριαίες τιμές είναι", data.shape[0] )
        # print("Οι έγκυρες ωριαίες τιμές είναι", data['SO2'].count())
        if data['SO2'].count() > (0.9*data.shape[0]):
            lines_of_new_file.append("Ικανοποιείται ο έλεγχος πληρότητας (90% των ωριαίων τιμών)")
            #print("Ικανοποιείται ο έλεγχος πληρότητας (90% των ωριαίων τιμών)")
        else:
            lines_of_new_file.append("Δεν ικανοποιείται ο έλεγχος πληρότητας (90% των ωριαίων τιμών)")
            #print("Δεν ικανοποιείται ο έλεγχος πληρότητας (90% των ωριαίων τιμών)")


        # In[215]:


        # Ωριαία οριακή τιμή (350μg/m3 έως 24 φορές τον χρόνο)

        if data.loc[data.SO2 > 350, 'SO2'].count() > 24 :
            lines_of_new_file.append("Γίνεται υπέρβαση του ορίου (350μg/m3 έως 24 φορές τον χρόνο)")
            #print("Γίνεται υπέρβαση του ορίου (350μg/m3 έως 24 φορές τον χρόνο)")
        else:
            lines_of_new_file.append("Δεν γίνεται υπέρβαση του ορίου στην ωριαία οριακή τιμή (350μg/m3) για πάνω από 24 φορές το έτος")
            #print("Δεν γίνεται υπέρβαση του ορίου στην ωριαία οριακή τιμή (350μg/m3) για πάνω από 24 φορές το έτος")

        countso2 = data.loc[data.SO2 > 350, 'SO2'].count()
        lines_of_new_file.append(f"Οι συνολικές υπερβάσεις ήταν: {countso2}")
        # print("Οι συνολικές υπερβάσεις ήταν:" ,data.loc[data.SO2 > 350, 'SO2'].count(),'\n')

        lines_of_new_file.append(" ")
        # In[111]:


        #Μέτρηση έγκυρων τιμών και έλεγχος πληρότητας για την ημερήσια οριακή τιμή (ημερήσιο)
        groups_so2 = len(grouped_single['SO2']) - grouped_single['SO2']['count'].le(18).sum()
        lines_of_new_file.append(f"Οι συνολικές ημερήσιες τιμές είναι {grouped_single.shape[0]}")
        lines_of_new_file.append(f"Οι έγκυρες ημερήσιες τιμές είναι {groups_so2}")
        # print("Οι συνολικές ημερήσιες τιμές είναι", grouped_single.shape[0] )
        # print("Οι έγκυρες ημερήσιες τιμές είναι", len(grouped_single['SO2']) - grouped_single['SO2']['count'].le(18).sum())

        if (len(grouped_single['SO2']) - grouped_single['SO2']['count'].le(18).sum()) > (0.9*data.shape[0]):
            lines_of_new_file.append("Ικανοποιείται ο έλεγχος πληρότητας (90% των ωριαίων τιμών)")
            #print("Ικανοποιείται ο έλεγχος πληρότητας")
        else:
            lines_of_new_file.append("Δεν ικανοποιείται ο έλεγχος πληρότητας")
            #print("Δεν ικανοποιείται ο έλεγχος πληρότητας")


        # In[112]:


        # Ημερήσια οριακή τιμή (125μg/m3 έως 3 φορές τον χρόνο)

        if grouped_single['SO2']['mean'].gt(125).sum() > 3:
            lines_of_new_file.append("Παρατηρείται υπέρβαση του ορίου (125μg/m3 έως 3 φορές τον χρόνο)")
            #print("Παρατηρείται υπέρβαση του ορίου (125μg/m3 έως 3 φορές τον χρόνο)")
        else:
            lines_of_new_file.append("Δεν παρατηρείται υπέρβαση του ορίου (125μg/m3 έως 3 φορές τον χρόνο)")
            #print("Δεν παρατηρείται υπέρβαση του ορίου (125μg/m3 έως 3 φορές τον χρόνο)")
        
        group_singl_mean = grouped_single['SO2']['mean'].gt(125).sum()
        lines_of_new_file.append(f"Συνολικές υπερβάσεις που παρατηρήθηκαν: {group_singl_mean}")
        #print("Συνολικές υπερβάσεις που παρατηρήθηκαν:", grouped_single['SO2']['mean'].gt(125).sum(), '\n')
        lines_of_new_file.append(" ")


        # # PM10

        # In[113]:

        lines_of_new_file.append("PM10")
        # print('PM10', '\n')
        lines_of_new_file.append(" ")


        # In[114]:


        #Έλεγχος πληρότητας (και για τα 2)
        lines_of_new_file.append(f"Συνολικές ημερήσιες τιμές: {grouped_single.shape[0]}")
        #print("Συνολικές ημερήσιες τιμές:", grouped_single.shape[0])
        checkpm10len =  len(grouped_single['PM10']) - grouped_single['PM10']['count'].le(18).sum()
        lines_of_new_file.append(f"Οι έγκυρες ημερήσιες τιμές είναι {checkpm10len}")
        #print("Οι έγκυρες ημερήσιες τιμές είναι", len(grouped_single['PM10']) - grouped_single['PM10']['count'].le(18).sum())

        if grouped_single['PM10']['mean'].count() > (0.9*grouped_single.shape[0]):
            lines_of_new_file.append("Ικανοποιείται ο έλεγχος πληρότητας (90% των ημερήσιων τιμών)")
            print("Ικανοποιείται ο έλεγχος πληρότητας (90% των ημερήσιων τιμών)")
        else:
            lines_of_new_file.append("Δεν ικανοποιείται ο έλεγχος πληρότητας (90% των ημερήσιων τιμών)")
            #print("Δεν ικανοποιείται ο έλεγχος πληρότητας (90% των ημερήσιων τιμών)")


        # In[115]:


        # Οριακή τιμή ημέρας = 50μg/m3 έως 35 φορές το έτος

        if grouped_single['PM10']['mean'].gt(50).sum() > 35:
            lines_of_new_file.append("Παρατηρείται υπέρβαση του ορίου (50μg/m3 έως 35 φορές το έτος)")
            #print("Παρατηρείται υπέρβαση του ορίου (50μg/m3 έως 35 φορές το έτος)")
        else:
            lines_of_new_file.append("Δεν παρατηρείται υπέρβαση του ορίου (50μg/m3 έως 35 φορές το έτος)")
            #print("Δεν παρατηρείται υπέρβαση του ορίου (50μg/m3 έως 35 φορές το έτος)")
        
        gr_sin_pm10 = grouped_single['PM10']['mean'].gt(50).sum()
        lines_of_new_file.append(f"Συνολικές υπερβάσεις που παρατηρήθηκαν: {gr_sin_pm10}")  
        #print("Συνολικές υπερβάσεις που παρατηρήθηκαν:", grouped_single['PM10']['mean'].gt(50).sum())


        # In[116]:


        # Οριακή τιμή έτους = 40 μg/m3 (με βάση τις ημερήσιες τιμές και όχι τις ωριαίες)
        gr_sing_mean = round(grouped_single['PM10']['mean'].mean(),1)
        lines_of_new_file.append(f"Η οριακή τιμή του έτους είναι {gr_sing_mean}")
        #print("Η οριακή τιμή του έτους είναι", round(grouped_single['PM10']['mean'].mean(),1), "μg/m3")

        if grouped_single['PM10']['mean'].mean() > 40:
            lines_of_new_file.append("Παρατηρείται παραβίαση του ορίου (40 μg/m3)")
            #print("Παρατηρείται παραβίαση του ορίου (40 μg/m3)",'\n')
        else:
            lines_of_new_file.append("Δεν παρατηρείται υπέρβαση του ορίου (40 μg/m3)")
            #print("Δεν παρατηρείται υπέρβαση του ορίου (40 μg/m3)",'\n')


        # # PM2.5

        # In[117]:

        lines_of_new_file.append(" ")
        lines_of_new_file.append('PM2.5')
        lines_of_new_file.append(" ")
        #print('PM2.5','\n')


        # In[118]:


        # Έλεγχος πληρότητας για το έτος 

        #print("Συνολικές τιμές:", grouped_single.shape[0])
        lines_of_new_file.append(f"Συνολικές τιμές: {grouped_single.shape[0]}")
        #print("Οι έγκυρες τιμές είναι", len(grouped_single['PM10']) - grouped_single['PM2.5']['count'].le(18).sum())
        gr_pm10_pm25 = len(grouped_single['PM10']) - grouped_single['PM2.5']['count'].le(18).sum()
        lines_of_new_file.append(f"Οι έγκυρες τιμές είναι {gr_pm10_pm25}")
        if len(grouped_single['PM10']) - grouped_single['PM2.5']['count'].le(18).sum() > (0.9*grouped_single.shape[0]):
            lines_of_new_file.append("Ικανοποιείται ο έλεγχος πληρότητας")
            #print("Ικανοποιείται ο έλεγχος πληρότητας")
        else:
            lines_of_new_file.append("Δεν ικανοποιείται ο έλεγχος πληρότητας")
            #print("Δεν ικανοποιείται ο έλεγχος πληρότητας")


        # In[119]:


        # Οριακή τιμή έτους = 25 μg/m3 (με βάση τις ημερήσιες τιμές και όχι τις ωριαίες)
        ch_pm25_mean = round(grouped_single['PM2.5']['mean'].mean(),1)
        lines_of_new_file.append(f"Η οριακή τιμή του έτους είναι {ch_pm25_mean}")
        #print("Η οριακή τιμή του έτους είναι", round(grouped_single['PM2.5']['mean'].mean(),1), "μg/m3")

        if grouped_single['PM2.5']['mean'].mean() > 25:
            lines_of_new_file.append("Παρατηρείται παραβίαση του ορίου (25 μg/m3)")
            #print("Παρατηρείται παραβίαση του ορίου (25 μg/m3)",'\n')
        else:
            lines_of_new_file.append("Δεν παρατηρείται υπέρβαση του ορίου 25 μg/m3")
            #print("Δεν παρατηρείται υπέρβαση του ορίου 25 μg/m3",'\n')


        # # O3

        # In[120]:

        lines_of_new_file.append(" ")
        lines_of_new_file.append('O3')
        lines_of_new_file.append(" ")
        #print('O3','\n')


        # In[207]:


        # Ημερήσιος μέσος όρος 8ώρου/ για να είναι τα δεδομένα πλήρη απαιτούνται 18 8ωροι μέσοι όροι για κάθε μέρα (75%)
        lines_of_new_file.append(f"Οι συνολικές τιμές (ημερήσιος μέσος όρος 8ώρου) είναι {grouped_single.shape[0]}")
        gr_le_o3roll = len(grouped_single['O3roll']) - grouped_single['O3roll']['count'].le(18).sum()
        #print(gr_le_o3roll)
        lines_of_new_file.append(f"Οι έγκυρες τιμές (18 8ωροι μέσοι όροι για κάθε μέρα (75%)) είναι {gr_le_o3roll}")
        #print("Οι συνολικές τιμές (ημερήσιος μέσος όρος 8ώρου) είναι", grouped_single.shape[0] )
        #print("Οι έγκυρες τιμές (18 8ωροι μέσοι όροι για κάθε μέρα (75%)) είναι", len(grouped_single['O3roll']) - grouped_single['O3roll']['count'].le(18).sum())

        #print(grouped_single['O3roll']['count'].le(18).sum())

        #pd.set_option('display.max_rows', 366)
        #print(grouped_single['O3roll']['count'])
        lines_of_new_file.append("different results from excel")
        #print('different results from excel')


        # In[122]:


        # Τιμή ορίου μέγιστου ημερήσιου 8ωρου: 120μg/m3 έως 25 φορές τον χρόνο σε διάστημα 3 χρόνων
        gr_o3roll_max = round(grouped_single['O3roll']['max'].max(), 1)
        lines_of_new_file.append(f"Η οριακή τιμή του έτους είναι {gr_o3roll_max} μg/m3")
        #print("Η οριακή τιμή του έτους είναι", round(grouped_single['O3roll']['max'].max(), 1), "μg/m3")

        if grouped_single['O3roll']['max'].max() > 120 and grouped_single['O3roll']['count'].gt(18): #25 
            lines_of_new_file.append("Παρατηρείται παραβίαση του ορίου (120μg/m3 έως 25 φορές τον χρόνο σε διάστημα 3 χρόνων)")
            #print("Παρατηρείται παραβίαση του ορίου (120μg/m3 έως 25 φορές τον χρόνο σε διάστημα 3 χρόνων)")
        else:
            lines_of_new_file.append("Δεν παρατηρείται υπέρβαση του ορίου (120μg/m3 έως 25 φορές τον χρόνο σε διάστημα 3 χρόνων)")
            #print("Δεν παρατηρείται υπέρβαση του ορίου (120μg/m3 έως 25 φορές τον χρόνο σε διάστημα 3 χρόνων)")
        
        grorollmaxsum = grouped_single['O3roll']['max'].gt(120).sum() 
        lines_of_new_file.append(f"Συνολικές παραβιάσεις μέγιστου ημερήσιου 8ώρου: {grorollmaxsum}")
        #print("Συνολικές παραβιάσεις μέγιστου ημερήσιου 8ώρου:", grouped_single['O3roll']['max'].gt(120).sum())


        # In[123]:


        # Όριο συναγερμού: 240 μg/m3 για 3 συνεχόμενες ώρες

        times = 0
        size = len(data['O3'])
        
        for i in range(size - 2):
        
            if data['O3'][i] > 240 and data['O3'][i + 1] > 240  and data['O3'][i + 2] > 240:
                times += 1
        lines_of_new_file.append(f"Yπερβάσεις του ορίου συναργερμού (240 μg/m3 για 3 συνεχόμενες ώρες) που παρατηρήθηκαν: {times}")    
        #print("Yπερβάσεις του ορίου συναργερμού (240 μg/m3 για 3 συνεχόμενες ώρες) που παρατηρήθηκαν:", times,'\n')


        # # CO

        # the data for this pollutant are not for the station of Pireus but for the station of Patision, Greece

        # In[124]:

        lines_of_new_file.append(" ")
        lines_of_new_file.append("CO")
        lines_of_new_file.append(" ")
        #print('CO','\n')


        # In[125]:


        # Ημερήσιος μέσος όρος 8ώρου/ για να είναι τα δεδομένα πλήρη απαιτούνται 18 8ωροι μέσοι όροι για κάθε μέρα (75%)
        lines_of_new_file.append(f"Οι συνολικές τιμές είναι {grouped_single.shape[0]}")
        gr_coroll = len(grouped_single['COroll']) - grouped_single['COroll']['count'].le(18).sum()
        lines_of_new_file.append(f"Οι έγκυρες τιμές είναι {gr_coroll}")
        #print("Οι συνολικές τιμές είναι", grouped_single.shape[0] )
        #print("Οι έγκυρες τιμές είναι", len(grouped_single['COroll']) - grouped_single['COroll']['count'].le(18).sum())

        #print(grouped_single['COroll']['count'].le(18).sum())

        pd.set_option('display.max_rows', 50)
        #print(grouped_single['COroll']['count'])
        lines_of_new_file.append("different results from excel")
        #print('different results from excel')


        # In[216]:


        # Τιμή ορίου μέγιστου ημερήσιου 8ωρου: 10mg/m3 
        cccc = round(grouped_single['COroll']['max'].max(), 2)
        lines_of_new_file.append(f"Η οριακή τιμή του έτους είναι {cccc} mg/m3")
        # print("Η οριακή τιμή του έτους είναι", round(grouped_single['COroll']['max'].max(), 2), "mg/m3")

        if grouped_single['COroll']['max'].max() > 10 and grouped_single['COroll']['count'].gt(18):
            lines_of_new_file.append("Παρατηρείται παραβίαση του ορίου (10mg/m3)")
            #print("Παρατηρείται παραβίαση του ορίου (10mg/m3)")
        else:
            lines_of_new_file.append("Δεν παρατηρείται υπέρβαση του ορίου (10mg/m3)")
            #print("Δεν παρατηρείται υπέρβαση του ορίου (10mg/m3)")
        gr_croll_maxx = grouped_single['COroll']['max'].gt(10).sum()
        lines_of_new_file.append(f"Συνολικές παραβιάσεις μέγιστου ημερήσιου 8ωρου: {gr_croll_maxx}")
        #print("Συνολικές παραβιάσεις μέγιστου ημερήσιου 8ωρου:", grouped_single['COroll']['max'].gt(10).sum(),'\n')
        #print(lines_of_new_file)

        nm = f'{namee}_Output.csv'
        with open(f'{namee}_Output.txt', 'w',encoding="UTF-8") as f:
            for line in lines_of_new_file:
                #l = str(line.encode("utf-8"))
                f.write(line)
                f.write('\n')
        #df = pd.read_csv(nm)
        #df.to_excel(f'{namee}_Output.xlsx', 'Sheet1')
        #os.remove(nm)
# In[ ]:




