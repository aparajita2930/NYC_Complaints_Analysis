source /home/$USER/.bashrc

rm use_cases_data/overall_complaint_type_dist.out
rm use_cases_data/year_dist.out
rm use_cases_data/year_complaint_type_dist.out
rm use_cases_data/hour_dist.out
rm use_cases_data/hour_complaint_type_dist.out
rm use_cases_data/day_dist.out
rm use_cases_data/day_complaint_type_dist.out
rm use_cases_data/status_dist.out
rm use_cases_data/overall_closing_time_dist.out
rm use_cases_data/agency_closing_time_dist.out
rm use_cases_data/complaint_closing_time_dist.out
rm use_cases_data/city_closing_time_dist.out
rm use_cases_data/borough_closing_time_dist.out
rm use_cases_data/bour_dist.out
rm use_cases_data/types_by_city.out
rm use_cases_data/types_by_location.out
rm use_cases_data/location_dist.out
hfs -rm -r overall_complaint_type_dist.out
hfs -rm -r year_dist.out
hfs -rm -r year_complaint_type_dist.out
hfs -rm -r hour_dist.out
hfs -rm -r hour_complaint_type_dist.out
hfs -rm -r day_dist.out
hfs -rm -r day_complaint_type_dist.out
hfs -rm -r status_dist.out
hfs -rm -r overall_closing_time_dist.out
hfs -rm -r agency_closing_time_dist.out
hfs -rm -r complaint_closing_time_dist.out
hfs -rm -r city_closing_time_dist.out
hfs -rm -r borough_closing_time_dist.out
hfs -rm -r bour_dist.out
hfs -rm -r types_by_city.out
hfs -rm -r types_by_location.out
hfs -rm -r location_dist.out

spark-submit use_cases/complaint_type_distribution.py /user/ac5901/NYC_Complaints.csv/part-00000
spark-submit use_cases/closing_time_distribution.py /user/ac5901/NYC_Complaints.csv/part-00000
spark-submit use_cases/status_distribution.py /user/ac5901/NYC_Complaints.csv/part-00000
spark-submit use_cases/comp_count.py /user/ac5901/NYC_Complaints.csv/part-00000
echo "-------------------------------------------------"
# or do whatever with individual element of the array
hfs -getmerge overall_complaint_type_dist.out use_cases_data/overall_complaint_type_dist.out
hfs -getmerge year_dist.out use_cases_data/year_dist.out
hfs -getmerge year_complaint_type_dist.out use_cases_data/year_complaint_type_dist.out
hfs -getmerge hour_dist.out use_cases_data/hour_dist.out
hfs -getmerge hour_complaint_type_dist.out use_cases_data/hour_complaint_type_dist.out
hfs -getmerge day_dist.out use_cases_data/day_dist.out
hfs -getmerge day_complaint_type_dist.out use_cases_data/day_complaint_type_dist.out
hfs -getmerge status_dist.out use_cases_data/status_dist.out
hfs -getmerge overall_closing_time_dist.out use_cases_data/overall_closing_time_dist.out
hfs -getmerge agency_closing_time_dist.out use_cases_data/agency_closing_time_dist.out
hfs -getmerge complaint_closing_time_dist.out use_cases_data/complaint_closing_time_dist.out
hfs -getmerge city_closing_time_dist.out use_cases_data/city_closing_time_dist.out
hfs -getmerge borough_closing_time_dist.out use_cases_data/borough_closing_time_dist.out
hfs -getmerge bour_dist.out use_cases_data/bour_dist.out
hfs -getmerge types_by_city.out use_cases_data/types_by_city.out
hfs -getmerge types_by_location.out use_cases_data/types_by_location.out
hfs -getmerge location_dist.out use_cases_data/location_dist.out


echo "-------------------------------------------------"
       


