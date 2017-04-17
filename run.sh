source /home/$USER/.bashrc

declare -a arr=("1_unique_key.py" "2_created_date.py" "3_closed_date.py" "4_agency.py" "5_agency_name.py" "6_complaint_type.py" "7_descriptor.py" "8_location_type.py" "9_incident_zip.py" "10_incident_address.py" "11_street_name.py" "12_cross_street_1.py" "13_cross_street_2.py" "14_intersection_street_1.py" "15_intersection_street_2.py" "16_address_type.py" "17_city.py" "18_landmark.py" "19_facility_type.py" "20_status.py" "21_due_date.py" "22_resolution_action_date.py" "23_community_board.py" "24_borough.py" "25_state_plane_x.py" "26_state_plane_y.py" "27_park_facility_name.py" "28_park_borough.py" "29_school_name.py" "30_school_number.py" "31_school_region.py" "32_school_code.py" "33_school_phone_number.py" "34_school_address.py" "35_school_city.py" "36_school_state.py" "37_school_zip.py" "38_school_not_found.py" "39_school_or_citywide_complaint.py" "40_vehicle_type.py" "41_taxi_company_borough.py" "42_taxi_pickup_location.py" "43_bridge_highway_name.py" "44_bridge_highway_direction.py" "45_road_ramp.py" "46_bridge_highway_segment.py" "47_garage_lot_name.py" "48_ferry_direction.py" "49_ferry_terminal_name.py" "50_latitude.py" "51_longitude.py" "52_location.py")

counter=1

for i in "${arr[@]}"
do
        echo "$i"
        echo "Deleting file : ${counter}_summary.out"
        rm out_data/${counter}_summary.out
        rm out_data/${counter}_datatype.out
        rm out_data/${counter}_semantictype.out
        rm out_data/${counter}_validity.out
        hfs -rm -r ${counter}_details.out
        hfs -rm -r ${counter}_summary.out
        hfs -rm -r ${counter}_datatype.out
        hfs -rm -r ${counter}_semantictype.out
        hfs -rm -r ${counter}_validity.out
        spark-submit src/column_summary/${i} /user/ac5901/NYC_Complaints.csv/part-00000
        echo "-------------------------------------------------"
        # or do whatever with individual element of the array
        spark-submit src/column_summary/0_create_file_summary.py ${counter}_details.out
        hfs -getmerge ${counter}_summary.out out_data/${counter}_summary.out
        spark-submit src/column_summary/0_column_summary.py ${counter}
        hfs -getmerge ${counter}_datatype.out out_data/${counter}_datatype.out
        hfs -getmerge ${counter}_semantictype.out out_data/${counter}_semantictype.out
        hfs -getmerge ${counter}_validity.out out_data/${counter}_validity.out
        echo "-------------------------------------------------"
        counter=$((counter+1))
done

