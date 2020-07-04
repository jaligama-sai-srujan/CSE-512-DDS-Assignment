#
# Assignment5 Interface
# Name: 
#

from pymongo import MongoClient
import os
import sys
import json
import math

def FindBusinessBasedOnCity(cityToSearch, saveLocation1, collection):
    output = collection.find()
    f = open(saveLocation1, "w")
    output_list = [] 
    for c in output:
        #print(cityToSearch,c["city"],cityToSearch==c["city"])
        if c["city"].upper() == cityToSearch.upper():
            output_list.append(c["name"].upper()+"$"+c["full_address"].upper()+"$"+c["city"].upper()+"$"+c["state"].upper())
    
    f.write("\n".join(output_list))
    f.close()
        
def dist(lat2, lon2, lat1, lon1):
    R = 3959; #miles
    l1 = math.radians(float(lat1))
    l2 = math.radians(float(lat2))
    lat_diff = math.radians(float(lat1)-float(lat2))
    lon_diff = math.radians(float(lon1)-float(lon2))
    a = math.sin(lat_diff/2) * math.sin(lat_diff/2) + math.cos(l1) * math.cos(l2) * math.sin(lon_diff/2) * math.sin(lon_diff/2);
    c = 2 * math.atan2(math.sqrt(a), math.sqrt(1-a));
    d = R * c;
    return d 
 
def FindBusinessBasedOnLocation(categoriesToSearch, myLocation, maxDistance, saveLocation2, collection):
    output = collection.find()
    for i in range(len(categoriesToSearch)):
        categoriesToSearch[i] = categoriesToSearch[i].upper()
    f = open(saveLocation2, "w")
    output_list= []
    for c in output:
        if dist(c["latitude"],c["longitude"],myLocation[0],myLocation[1])<=maxDistance:
            for category in c["categories"]:
                if category.upper() in categoriesToSearch:
                    output_list.append(c["name"].upper())
                    break
    f.write("\n".join(output_list))
    f.close()
    
            


 
 