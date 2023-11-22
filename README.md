# 데이터 구조

![Data Flow](https://github.com/DJMLteam2/DP_DB/assets/135206238/97a8c68f-7b53-4fff-860c-056753aea787)

# 사용방법
팀 DB
AWS RDS
HOST : database-1.ckodcngpvqn9.ap-northeast-2.rds.amazonaws.com
USERNAME : admin
PORT : 3306
PW : 조장에게 문의

# 사용방법
1) 데이터 다운로드  
AIHUB 여행 데이터 링크: https://www.aihub.or.kr/aihubdata/data/view.do?currMenu=115&topMenu=100&dataSetSn=71581  
이미지를 제외한 데이터 끌고 오기  
2) 데이터 적재
GPS 데이터를 제외한 나머지 데이터를 아래의 그림과 같이 적재해둘 것
- 파일구조 예시  
![image](https://github.com/DJMLteam2/DP_DB/assets/135789538/1bb5a664-3b09-4324-9a48-d4e59d0284af)

3) 데이터 DB 사용 구문
```
select count(*)
from ai_hub_visit_area_db v
  Inner JOIN ai_hub_travel_db t
  ON v.TRAVEL_ID = t.TRAVEL_ID
  Inner Join ai_hub_person_info_db p
  ON t.TRAVELER_ID = p.TRAVELER_ID
  inner join ai_hub_faver_info_db f
  on  p.TRAVELER_ID = f.TRAVELER_ID
Where 
  Not v.VISIT_AREA_NM Like "%공항%" and
  Not v.VISIT_AREA_NM = "친구/친지집" and
  Not v.VISIT_AREA_NM = "사무실" and
  v.X_COORD Is not null and 
  v.Y_COORD is not null;
```
