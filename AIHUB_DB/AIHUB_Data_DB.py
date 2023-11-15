import pandas as pd
import os
import numpy as np

##############################################################
### AIHUB_CSV 클래스
### AIHUB에서 수집된 데이터를 전처리하여 csv 파일로 만드는 클래스
##############################################################
class AIHUB_CSV:
    #######################################################
    ### 저장경로: 경로가 올바르지 않을 경우 이곳에서 수정 ###
    #######################################################
    Train_Folder_path = "./Origin_Data/Travel/{}/Training/TL_csv/"
    Val_Folder_path = "./Origin_Data/Travel/{}/Validation/VL_csv/"
    Save_path = "./Origin_Data/Result/"

    ##################
    ### 초기데이터 ###
    ##################
    AreaCode = ""
    Aihub_df_List = []
    code_csv = None;
    sgg_csv = None;

    def __init__(self, Area, AreaCode):
        self.Train_Folder_path = self.Train_Folder_path.format(Area)
        self.Val_Folder_path = self.Val_Folder_path.format(Area)
        self.AreaCode = AreaCode
        self.code_csv = pd.read_csv(self.Train_Folder_path + "tc_codeb_코드B.csv", encoding="utf-8-sig")
        self.sgg_csv = pd.read_csv(self.Train_Folder_path + "tc_sgg_시군구코드.csv", encoding="utf-8-sig")

    ##################################
    ### 데이터 셋 준비 및 저장 함수 ###
    ##################################
    def AIHUB_DATA(self):
        train_csv_list = self.EXTRACT_DATA(self.Train_Folder_path)
        val_csv_list = self.EXTRACT_DATA(self.Val_Folder_path)
        for train_csv, val_csv in zip(train_csv_list, val_csv_list):
            aihub_df = pd.concat([train_csv, val_csv])
            aihub_df.drop_duplicates()
            self.Aihub_df_List.append(aihub_df)
    
    def EXTRACT_DATA(self, Folder_path):
        travel_csv = pd.read_csv(Folder_path + f"tn_travel_여행_{self.AreaCode}.csv", encoding="utf-8-sig")
        traveler_csv = pd.read_csv(Folder_path + f"tn_traveller_master_여행객 Master_{self.AreaCode}.csv", encoding="utf-8-sig")
        visit_area_csv = pd.read_csv(Folder_path + f"tn_visit_area_info_방문지정보_{self.AreaCode}.csv", encoding="utf-8-sig");
        return [travel_csv, traveler_csv, visit_area_csv]
    
    def SAVE_DATA(self, travel, person, faver,visit):
        travel.to_csv(self.Save_path + "AIHUB_travel_DB.csv", index=False, encoding="utf-8-sig")
        person.to_csv(self.Save_path + "AIHUB_person_info_DB.csv", index=False, encoding="utf-8-sig")
        faver.to_csv(self.Save_path + "AIHUB_faver_info_DB.csv", index=False, encoding="utf-8-sig")
        visit.to_csv(self.Save_path + "AIHUB_visit_DB.csv", index=False, encoding="utf-8-sig")

    ############################
    ### 데이터 칼럼 정리 함수 ###
    ############################
    def TRAVEL_DATA(self):
        travel = self.Aihub_df_List[0].copy()
        drop_columns = ["TRAVEL_MISSION", "TRAVEL_MISSION_CHECK"]
        travel = travel.drop(columns=drop_columns)
        
        travel["TRAVEL_PURPOSE"] = travel["TRAVEL_PURPOSE"].apply(self.TRAVEL_PURPOSE_INDEX)
        travel = travel.replace({np.NaN:None})
        return travel
    
    def TRAVELER_DATA(self):
        traveler = self.Aihub_df_List[1].copy()
        return self.TRAVELER_PERSON_INFO_DATA(traveler), self.TRAVELER_PERSON_FAVER_DATA(traveler)
    
    def TRAVELER_PERSON_INFO_DATA(self, traveler):
        # 필드 추출
        info_columns = list(traveler.columns[0:14]) 
        info_columns.append(traveler.columns[-1])
        info_columns.append("TRAVEL_STATUS_YMD")
        person = traveler[info_columns].copy()

        # 전처리
        filter_columns = ["EDU_NM","EDU_FNSH_SE","MARR_STTS","JOB_NM"
              , "JOB_ETC", "INCOME", "HOUSE_INCOME","TRAVEL_TERM"]
        code_columns = ["EDU","EFS","MAR","JOB","JOE","INC","INC","TTM"]
        for filter, code in zip(filter_columns, code_columns):
            person[filter] = person[filter].apply(self.TRANSFOR_CODE, args=[code])

        person["RESIDENCE_SGG_CD"] = person["RESIDENCE_SGG_CD"].apply(self.TRANSFOR_SGG)

        people_datetime = person["TRAVEL_STATUS_YMD"].str.split("~")
        person["TRAVEL_STATUS_START_DAY"] = people_datetime.str.get(0)
        person["TRAVEL_STATUS_END_DAY"] = people_datetime.str.get(1)
        person = person.drop(columns=["TRAVEL_STATUS_YMD"])
    
        person = person.replace({np.NaN:None})
        return person
    
    def TRAVELER_PERSON_FAVER_DATA(self, traveler):
        # 필드 추출
        faver_columns = []
        faver_columns.append(traveler.columns[0])
        faver_columns.extend(list(traveler.columns[14:-3]))
        faver_columns.remove("TRAVEL_STATUS_YMD")
        faver = traveler[faver_columns].copy()
        
        # 전처리
        code_columns = ["TRAVEL_LIKE_SIDO_1", "TRAVEL_LIKE_SGG_1", "TRAVEL_LIKE_SIDO_2", "TRAVEL_LIKE_SGG_2", "TRAVEL_LIKE_SIDO_3", "TRAVEL_LIKE_SGG_3"]
        for code_column in code_columns:
            faver[code_column] = faver[code_column].apply(self.TRANSFOR_SGG)
        faver["TRAVEL_MOTIVE_1"] = faver["TRAVEL_MOTIVE_1"].apply(self.TRANSFOR_CODE, args=["TMT"])
        return faver
    
    def VISIT_DATA(self):
        # 필드 추출
        visit = self.Aihub_df_List[2].copy()
        drop_columns = ["ROAD_NM_CD", "LOTNO_CD","POI_ID", "POI_NM","LODGING_TYPE_CD","SGG_CD"]
        visit = visit.drop(columns=drop_columns)

        # 전처리
        code_list = ["VIS", "REN"]
        change_columns = ["VISIT_AREA_TYPE_CD", "VISIT_CHC_REASON_CD"]
        for code, change_column in zip(code_list, change_columns):
            visit[change_column] = visit[change_column].apply(self.TRANSFOR_CODE, args=[code])
        visit = visit.replace({np.NaN:None})
        return visit
    
    ############################
    ### 칼럼 정리 전처리 함수 ###
    ############################
    def TRAVEL_PURPOSE_INDEX(self, x):
        """
        Travel 중 TRAVEL_PURPOSE 코드화된 다중 데이터를 시각화 하는 함수
        """
        purpose_list = x.split(";")
        purpose_list = purpose_list[:len(purpose_list) - 1]
        purpose_transfrom_list = []
        for purpose in purpose_list:
            purpose_transfrom = self.code_csv["cd_nm"][(self.code_csv["cd_a"] == "MIS") & (self.code_csv["cd_b"] == purpose)]
            purpose_transfrom_list.append(purpose_transfrom)
        purpose_transfrom_list = np.concatenate(purpose_transfrom_list).tolist()
        return ",".join(purpose_transfrom_list)
    
    def TRANSFOR_CODE(self, codeNum, codeName):
        """
        코드화된 데이터를 시각화 하는 함수
        """
        try:
            codeNum = int(codeNum)
            result = self.code_csv["cd_nm"][(self.code_csv["cd_a"] == codeName) & (self.code_csv["cd_b"] == str(codeNum))]
            result = " ".join(result.values.tolist())
        except:
            result = np.NaN;
        return result
    
    def TRANSFOR_SGG(self, codeNum):
        """
        시군구 코드의 데이터를 시각화 하는 함수
        """
        try:
            length = len(str(int(codeNum)))  
            codeNum = codeNum * (10 ** (10 - length))
            result = " ".join(np.concatenate(self.sgg_csv[["SIDO_NM", "SGG_NM", "DONG_NM"]][self.sgg_csv["SGG_CD"] == codeNum].dropna(axis=1).values).tolist())
        except:
            result = np.NaN
        return result
    
    ###########################
    ### AIHUB_CSV 메인 함수 ###
    ###########################

    def AIHUB_CSV_MAIN(self):
        self.AIHUB_DATA();
        travel = self.TRAVEL_DATA();
        person, faver = self.TRAVELER_DATA();
        visit = self.VISIT_DATA();
        self.SAVE_DATA(travel, person, faver, visit);

import pymysql
import time

################################################
### AIHUB_DB 클래스
### AIHUB에서 생성된 csv 파일을 DB화 시키는 클래스
################################################
class AIHUB_DB:
    ######################################################
    ### csv정보: 경로가 올바르지 않을 경우 이곳에서 수정 ###
    ######################################################
    Folder_path = "./Origin_Data/Result/"
    File_Name = [
        "AIHUB_travel_DB.csv",
        "AIHUB_person_info_DB.csv",
        "AIHUB_faver_info_DB.csv",
        "AIHUB_visit_DB.csv"
    ]

    ###############
    ### DB 정보 ###
    ###############
    DB_USER = 'root'
    DB_PASSWD = 'sql4869'
    DB_HOST ='127.0.0.1' # loopback # localhost
    DB_PORT = 3306
    DB_NAME = 'sam_db' # DB 만들면 그거 이름

    con = None
    cur = None

    def __init__(self):
        try:
            self.con = pymysql.connect(host=self.DB_HOST, user=self.DB_USER, password=self.DB_PASSWD, port=self.DB_PORT, db=self.DB_NAME, charset='utf8')
            self.cur = self.con.cursor()
        except Exception as e:
            print(e)
            print("DB 접속 실패");
    
    def __del__(self):
        self.cur.close();
        self.con.close();
    
    def CSV_To_INPUTDATA(self):
        travel = self.CSV_To_DataFrame(self.Folder_path + self.File_Name[0])
        person = self.CSV_To_DataFrame(self.Folder_path + self.File_Name[1])
        faver = self.CSV_To_DataFrame(self.Folder_path + self.File_Name[2])
        visit = self.CSV_To_DataFrame(self.Folder_path + self.File_Name[3])
        return [person, faver, travel, visit]

    def CSV_To_DataFrame(self, File_Path):
        df = pd.read_csv(File_Path, encoding="utf-8-sig")
        df = df.replace({np.NaN:None})
        return df
    
    def AIHUB_SQL(self):
        ################
        ### DB SQL문 ###
        ################
        travel_sql ="""
        CREATE TABLE AI_HUB_travel_DB(
            TRAVEL_ID VARCHAR(255) PRIMARY KEY NOT NULL,
            TRAVEL_NM VARCHAR(255),
            TRAVELER_ID VARCHAR(255),
            TRAVEL_PURPOSE VARCHAR(255),
            TRAVEL_START_YMD DATE,
            TRAVEL_END_YMD DATE,
            MVMN_NM VARCHAR(255),
            TRAVEL_PERSONA VARCHAR(255)
        )
        # TRAVEL_ID TRAVEL_NM TRAVELER_ID TRAVEL_PURPOSE
        # TRAVEL_START_YMD TRAVEL_END_YMD MVMN_NM TRAVEL_PERSONA
        """
        travel_sql_input = """
        INSERT INTO AI_HUB_travel_DB VALUES(%s, %s, %s, %s, %s, %s, %s, %s)
        """

        # 2. 여행객 DB [사람정보]
        pepole_info_sql = """
        CREATE TABLE AI_HUB_person_info_DB(
            TRAVELER_ID VARCHAR(255) PRIMARY KEY NOT NULL,
            RESIDENCE_SGG_CD VARCHAR(255),
            GENDER VARCHAR(255),
            AGE_GRP int(10),
            EDU_NM VARCHAR(255),
            EDU_FNSH_SE VARCHAR(255),
            MARR_STTS VARCHAR(255),
            FAMILY_MEMB int(10),
            JOB_NM VARCHAR(255),
            JOB_ETC VARCHAR(255),
            INCOME VARCHAR(255),
            HOUSE_INCOME VARCHAR(255),
            TRAVEL_TERM VARCHAR(255),
            TRAVEL_NUM int(10),
            TRAVEL_COMPANIONS_NUM int(10),
            TRAVEL_STATUS_START_DAY DATE,
            TRAVEL_STATUS_END_DAY DATE
        )
        """
        # TRAVELER_ID RESIDENCE_SGG_CD GENDER AGE_GRP EDU_NM
        # EDU_FNSH_SE MARR_STTS	FAMILY_MEMB	JOB_NM	JOB_ETC	
        # INCOME HOUSE_INCOME TRAVEL_TERM TRAVEL_NUM TRAVEL_COMPANIONS_NUM
        # TRAVEL_STATUS_START_DAY TRAVEL_STATUS_END_DAY
        pepole_info_sql_input = """
        INSERT INTO AI_HUB_person_info_DB VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """

        # 3. 여행객 DB [취향조사]
        faver_info_sql = """
        CREATE TABLE AI_HUB_faver_info_DB(
            TRAVELER_ID VARCHAR(255),
            TRAVEL_LIKE_SIDO_1 VARCHAR(255),
            TRAVEL_LIKE_SGG_1 VARCHAR(255),
            TRAVEL_LIKE_SIDO_2 VARCHAR(255),
            TRAVEL_LIKE_SGG_2 VARCHAR(255),
            TRAVEL_LIKE_SIDO_3 VARCHAR(255),
            TRAVEL_LIKE_SGG_3 VARCHAR(255),
            TRAVEL_STYL_1 int(10),
            TRAVEL_STYL_2 int(10),
            TRAVEL_STYL_3 int(10),
            TRAVEL_STYL_4 int(10),
            TRAVEL_STYL_5 int(10),
            TRAVEL_STYL_6 int(10),
            TRAVEL_STYL_7 int(10),
            TRAVEL_STYL_8 int(10),
            TRAVEL_STATUS_RESIDENCE VARCHAR(255),
            TRAVEL_STATUS_DESTINATION VARCHAR(255),
            TRAVEL_STATUS_ACCOMPANY VARCHAR(255),
            TRAVEL_MOTIVE_1 VARCHAR(255),
            foreign key (TRAVELER_ID) references AI_HUB_person_info_DB(TRAVELER_ID)
        )
        """

        faver_info_sql_input = """
        INSERT INTO AI_HUB_faver_info_DB 
        VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """

        # 5. 방문지 정보
        visit_area_sql = """
        CREATE TABLE AI_HUB_visit_area_DB(
            VISIT_AREA_ID VARCHAR(255),
            TRAVEL_ID VARCHAR(255),
            VISIT_ORDER int(10),
            VISIT_AREA_NM VARCHAR(255),
            VISIT_START_YMD DATE,
            VISIT_END_YMD DATE,
            ROAD_NM_ADDR VARCHAR(255),
            LOTNO_ADDR VARCHAR(255),
            X_COORD DECIMAL(10, 7),
            Y_COORD DECIMAL(10, 7),
            RESIDENCE_TIME_MIN DECIMAL(10,2),
            VISIT_AREA_TYPE_CD VARCHAR(255),
            REVISIT_YN VARCHAR(255),
            VISIT_CHC_REASON_CD VARCHAR(255),
            DGSTFN DECIMAL(10,2),
            REVISIT_INTENTION DECIMAL(10,2),
            RCMDTN_INTENTION DECIMAL(10,2)
        )
        """
        # VISIT_AREA_ID TRAVEL_ID VISIT_ORDER VISIT_AREA_NM VISIT_START_YMD	
        # VISIT_END_YMD ROAD_NM_ADDR LOTNO_ADDR X_COORD Y_COORD	
        # RESIDENCE_TIME_MIN VISIT_AREA_TYPE_CD REVISIT_YN VISIT_CHC_REASON_CD DGSTFN
        # REVISIT_INTENTION RCMDTN_INTENTION
        visit_area_sql_input = """
        INSERT INTO AI_HUB_visit_area_DB 
        VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """

        #########################
        ### SQL 정보 딕셔너리 ###
        #########################
        create_db_sql_dic = {
            "AI_HUB_person_info_DB":pepole_info_sql, 
            "AI_HUB_faver_info_DB":faver_info_sql, 
            "AI_HUB_travel_DB":travel_sql,
            "AI_HUB_visit_area_DB":visit_area_sql
        }
        insert_db_sql_dic = {
            "AI_HUB_person_info_DB":pepole_info_sql_input, 
            "AI_HUB_faver_info_DB":faver_info_sql_input,
            "AI_HUB_travel_DB":travel_sql_input, 
            "AI_HUB_visit_area_DB":visit_area_sql_input
        }

        return create_db_sql_dic, insert_db_sql_dic

    #############################
    ### AIHUB DB 생성 및 입력 ###
    #############################
    def AIHUB_DATABASE_CREATE(self, create_db_sql_dic):
        for db_name, create_db_sql in create_db_sql_dic.items():
            delete_db_sql = f"Delete from {db_name};"
            if not self.Check_DATABASE(db_name):
                self.CREATE_DATABASE(create_db_sql)
            else:
                self.DELETE_DATABASE(delete_db_sql)

    def AIHUB_DATABASE_INSERT(self, create_db_sql_dic, insert_db_sql_dic, input_data_list):
        for sql_dic, df in zip(insert_db_sql_dic.items(), input_data_list):
            db_name, insert_db_sql = sql_dic
            print(db_name)
            if not self.Check_DATABASE(db_name):
                create_db_sql = create_db_sql_dic.get(db_name)
                self.CREATE_DATABASE(create_db_sql)
            self.Insert_DATABASE(insert_db_sql, df.to_records().tolist())
            time.sleep(5)
    
    ##############
    ### DML 문 ###
    ##############
    def CREATE_DATABASE(self, create_db_sql):
        try:
            self.cur.execute(create_db_sql)
        except Exception as e:
            print(e)
            print("테이블 생성실패")

    def DELETE_DATABASE(self, delete_db_sql):
        try:
            self.cur.execute(delete_db_sql)
        except Exception as e:
            print(e)
            print("삭제실패")

    def Insert_DATABASE(self, insert_db_sql, insert_data):
        try:
            for data in insert_data:
                try:
                    self.cur.execute(insert_db_sql, data[1:])
                except Exception as e:
                    print(data[1:])
                    print(e)
                    print("Insert 실패")
            self.con.commit();
        except:
            print("DB등록 실패")
    
    def Check_DATABASE(self, db_name):
        check_db_sql = f"SHOW TABLES LIKE \'{db_name}\';"
        try:
            return self.cur.execute(check_db_sql);
        except Exception as e:
            print("확인 실패");
            exit();

    ##########################
    ### AIHUB_DB 메인 함수 ###
    ##########################

    def AIHUB_DB_MAIN(self):
        input_data_list = self.CSV_To_INPUTDATA();
        create_db_sql_dic, insert_db_sql_dic = self.AIHUB_SQL();
        self.AIHUB_DATABASE_CREATE(create_db_sql_dic);
        self.AIHUB_DATABASE_INSERT(create_db_sql_dic, insert_db_sql_dic, input_data_list);

if __name__ == "__main__":
    a = AIHUB_CSV("Jeju", "D");
    a.AIHUB_CSV_MAIN();
    b = AIHUB_DB();
    b.AIHUB_DB_MAIN();