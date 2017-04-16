import os
import string
import numpy as np
import matplotlib.pyplot as plt


def read_out(file):
    content = open(file).read().strip().split("\n")
    content = [x.split("\t") for x in content]
    return np.array(content)

def miltary_hour(hour):
    if hour == "12AM":
        return 0
    elif hour == "12PM":
        return 12
    else:
        time = 0
        if hour[-2:] == "PM":
            time += 12
            
        time += int(hour[:2])
        
        return time

npfloat = np.vectorize(float)

results_DIR = "../results/res_use_cases/"

for file in os.listdir(results_DIR):
    
    if file == "borough_closing_time_dist.out":
        file_path = results_DIR+file
        table = read_out(file_path)
        values =  table[:, 1]
        ind = np.arange(len(values))
        width = 0.5
        fig, ax = plt.subplots()
        fig.set_size_inches(8.5, 4.5)
        ax.bar(2*ind + .5*width, npfloat(values), width, color='g')
        ax.set_ylabel("Avg Closing Time")
        ax.set_xticks(2*ind + width )
        ax.set_xticklabels(table[:, 0])
        bor_avg = npfloat(values)
        plt.savefig("graphs/"+ file.strip(".out")+"_bargraph")
        plt.clf()
        
    if file == "agency_closing_time_dist.out":
        file_path = results_DIR+file
        table = read_out(file_path)[:11]
        values =  table[:, 1]
        ind = np.arange(len(values))
        width = 1
        fig, ax = plt.subplots()
        fig.set_size_inches(8.5, 4.5)
        ax.bar(2*ind + .5*width, npfloat(values), width, color='g')
        ax.set_ylabel("Avg Closing Time")
        ax.set_xticks(2*ind + width )
        ax.set_xticklabels(table[:, 0])
        bor_avg = npfloat(values)
        plt.savefig("graphs/"+ file.strip(".out")+"_top10_bargraph")
        plt.clf()
        
    if file == "bour_dist.out":
        file_path = results_DIR+file
        table = read_out(file_path)
        values =  table[:, 1]
        ind = np.arange(len(values))
        width = 0.5
        fig, ax = plt.subplots()
        fig.set_size_inches(8.5, 4.5)
        ax.bar(2*ind + .5*width, npfloat(values), width, color='g')
        ax.set_ylabel("Number of complaints")
        ax.set_xticks(2*ind + width )
        ax.set_xticklabels(table[:, 0])
        bor_avg = npfloat(values)
        plt.savefig("graphs/"+ file.strip(".out")+"_bargraph")
        plt.clf()
        
    if file == "city_closing_time_dist.out":
        file_path = results_DIR+file
        table = read_out(file_path)[:10]
        values =  table[:, 1]
        ind = np.arange(len(values))
        width = 1
        fig, ax = plt.subplots()
        fig.set_size_inches(20.5, 4.5)
        ax.bar(2*ind + .5*width, npfloat(values), width, color='g')
        ax.set_ylabel("Avg Closing Time")
        ax.set_xticks(2*ind + width )
        ax.set_xticklabels(table[:, 0])
        bor_avg = npfloat(values)
        plt.savefig("graphs/"+ file.strip(".out")+"_top10_bargraph")
        plt.clf()
        
    if file == "complaint_closing_time_dist.out":
        file_path = results_DIR+file
        table = read_out(file_path)[:10]
        values =  table[:, 1]
        ind = np.arange(len(values))
        width = 1
        fig, ax = plt.subplots()
        fig.set_size_inches(18.5, 4.5)
        ax.bar(2*ind + .5*width, npfloat(values), width, color='g')
        ax.set_ylabel("Avg Closing Time")
        ax.set_xticks(2*ind + width )
        ax.set_xticklabels(table[:, 0], rotation='vertical')
        bor_avg = npfloat(values)
        plt.savefig("graphs/"+ file.strip(".out")+"_top10_bargraph")
        plt.clf()
        
    if file == "day_complaint_type_dist.out":
        file_path = results_DIR+file
        table = read_out(file_path)
        types = []
        width = 0.5
        for key in table[:, 0]:
            if key.split(",")[1] in types:
                pass
            else:
                types.append(key.split(",")[1])
        
        for comp in types:
            type_table = {}
            for element in table:
                key = element[0]
                value = element[1]
                if comp in key:
                    day = key.split(",")[0]
                    type_table[day] = float(value)
                else:
                    pass
                
            else:
                plt.bar(range(len(type_table)), type_table.values(), width,
                        color='g', align='center')
                plt.xticks(range(len(type_table)), type_table.keys())
                plt.title("Distribution of %s complaints per weekday" % (comp))
                plt.ylabel("Number of complaints")
                plt.savefig("graphs/"+ "day_type_dist_"+(comp.replace("/", "")
                                              .replace(".", "").replace(" ", "_")
                                              .replace("-", "_")))
                plt.clf()
                
    if file == "day_dist.out":
        file_path = results_DIR+file
        table = read_out(file_path)
        values =  table[:, 1]
        ind = np.arange(len(values))
        width = 0.5
        fig, ax = plt.subplots()
        fig.set_size_inches(8.5, 4.5)
        ax.bar(2*ind + .5*width, npfloat(values), width, color='g')
        ax.set_ylabel("Number of complaints")
        ax.set_xticks(2*ind + width )
        ax.set_xticklabels(table[:, 0])
        bor_avg = npfloat(values)
        plt.savefig("graphs/"+ file.strip(".out")+"_bargraph")
        plt.clf()
        
    if file == "hour_complaint_type_dist.out":
        file_path = results_DIR+file
        table = read_out(file_path)
        types = []
        width = 0.5
        for key in table[:, 0]:
            if key.split(",")[1] in types:
                pass
            else:
                types.append(key.split(",")[1])
        
        for comp in types:
            type_table = np.zeros(24)
            for element in table:
                key = element[0]
                value = element[1]
                if comp in key:
                    hour = key.split(",")[0]
                    hour = miltary_hour(hour)
                    type_table[hour] = float(value)
                else:
                    pass
                
            else:
                plt.plot(range(len(type_table)), type_table, '-g')
                plt.title("Distribution of %s complaints per hour" % (comp))
                plt.ylabel("Number of complaints")
                plt.xlabel("Hour")
                plt.savefig("graphs/"+ "hourly_type_dist_"+(comp.replace("/", "")
                                              .replace(".", "").replace(" ", "_")
                                              .replace("-", "_")))
                plt.clf()
                
                
    if file == "hour_dist.out":
        file_path = results_DIR+file
        table = read_out(file_path)
        hour_table = np.zeros(24)
        for element in table:
            hour = element[0]
            value = element[1]
            hour = miltary_hour(hour)
            hour_table[hour] = float(value)
        
        plt.plot(range(len(hour_table)), hour_table, '-g')
        plt.title("Distribution of complaints per hour")
        plt.ylabel("Number of complaints")
        plt.xlabel("Hour")
        plt.savefig("graphs/"+ "hourly_comp_dist")
        plt.clf()
        
        
    if file == "location_dist.out":
        file_path = results_DIR+file
        table = read_out(file_path)[:10]
        values =  table[:, 1]
        ind = np.arange(len(values))
        width = 1
        fig, ax = plt.subplots()
        fig.set_size_inches(20.5, 4.5)
        ax.bar(2*ind + .5*width, npfloat(values), width, color='g')
        ax.set_ylabel("Number of Complaints")
        ax.set_xticks(2*ind + width )
        ax.set_xticklabels(table[:, 0])
        bor_avg = npfloat(values)
        plt.savefig("graphs/"+ file.strip(".out")+"_top10_bargraph")
        plt.clf()
        
        
    if file == "overall_complaint_type_dist.out":
        file_path = results_DIR+file
        table = read_out(file_path)[:10]
        values =  table[:, 1]
        ind = np.arange(len(values))
        width = 1
        fig, ax = plt.subplots()
        fig.set_size_inches(20.5, 4.5)
        ax.bar(2*ind + .5*width, npfloat(values), width, color='g')
        ax.set_ylabel("Number of Complaints")
        ax.set_xticks(2*ind + width )
        ax.set_xticklabels(table[:, 0], rotation='vertical')
        bor_avg = npfloat(values)
        plt.savefig("graphs/"+ file.strip(".out")+"_top10_bargraph")
        plt.clf()
        
    if file == "year_complaint_type_dist.out":
        file_path = results_DIR+file
        table = read_out(file_path)
        types = []
        width = 0.5
        for key in table[:, 0]:
            if key.split(",")[1] in types:
                pass
            else:
                types.append(key.split(",")[1])
        
        for comp in types:
            type_table = np.zeros(2017-2009+1)
            for element in table:
                key = element[0]
                value = element[1]
                if comp in key:
                    year = key.split(",")[0]
                    year = int(year)
                    type_table[year-2009] = float(value)
                else:
                    pass
                
            else:
                plt.plot(range(2009, 2018), type_table, '-g')
                plt.title("Distribution of %s complaints by year" % (comp))
                plt.ylabel("Number of complaints")
                plt.xlabel("Hour")
                plt.savefig("graphs/"+ "hourly_type_dist_"+(comp.replace("/", "")
                                              .replace(".", "").replace(" ", "_")
                                              .replace("-", "_")))
                plt.clf()
                
    if file == "year_dist.out":
        file_path = results_DIR+file
        table = read_out(file_path)
        year_table = np.zeros(2017-2009+1)
        for element in table:
            year = element[0]
            value = element[1]
            year = int(year)
            year_table[year-2009] = float(value)
            
        plt.plot(range(2009, 2018), year_table, "-g")
        plt.title("Distribution of complaints by year")
        plt.ylabel("Number of complaints")
        plt.xlabel("Year")
        plt.savefig("graphs/"+ "yearly_comp_dist")
        plt.clf()