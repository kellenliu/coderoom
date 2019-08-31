
--this is test

--数据导出
insert overwrite local directory '/mnt/disk1/user/lkl/results/target_prelevel1'
row format delimited fields terminated by ',' 
select *
from workspace.lkl_target_user_ring_video_type_201908_a 

--数据导出
bash /mnt/disk1/share/bin/merge_reduce.sh /mnt/disk1/user/lkl/results/target_prelevel1



--音视频类型分类
drop table workspace.lkl_video_app_fenlei_201908_d;
use workspace;
create table workspace.lkl_video_app_fenlei_201908_d
    row format delimited fields terminated by '\001' 
    stored as orc tblproperties ('orc.compress'='ZLIB') 
as(
        select app_id, app_name
        ,case when app_name in 
        (
            '微视','快手','爆米花网','小影视频','西瓜视频','火山小视频',
        '抖音短视频','小影视频','微视','Gif快手','YouTube视频','爱拍','YouTube',
        '六间房','小咖秀','熊猫TV','秒拍','抖音','HuoShanXiaoShiPin','bilibili'
        ) then '短视频'
        when app_name in
        (
            '酷6视频','搜狐视频','新浪视频','网易视频','乐视视频','56视频','央视影音','乐视网络电视',
        '1905电影网','百度视频','360影视大全','华数TV','哔哩哔哩动画','天翼视讯','百视通','AcFun',
        '四川广播电视台','中国蓝TV','四川广播电视台','韩剧TV','人人视频','哔哩哔哩动画','56视频',
        'CNTV','和视频','优酷网','网易视频','华数TV','中国蓝TV','AcFun','乐视视频','土豆网','四川广播电视台',
        '酷6网','百度视频','360影视大全','搜狐视频','新浪视频','咪咕视频','优酷','土豆视频','腾讯视频','芒果TV',
        '爱奇艺视频','MLB TV','PandoraTV','奇艺影音','爱奇艺','腾信视频'
        ) then '长视频'
        when app_name in 
        (
            '开迅视频','斗鱼TV','虎牙直播','龙珠直播','战旗直播','章鱼TV','来疯直播秀','95美女秀','繁星直播',
        '易直播','花椒直播','全民TV','目睹直播','咪咕直播','企鹅直播','一直播','虎牙直播','龙珠直播',
        '战旗直播','花椒直播','一直播','斗鱼TV','龙珠直播','来疯直播秀','虎牙直播','章鱼TV','95美女秀',
        '繁星直播','易直播','花椒直播','全民TV','目睹直播','咪咕直播','开迅视频','风云直播','龙龙直播',
        '直播吧女神','艾米直播','TianTianZhiBo','360直播网','果酱直播','EF英孚教育直播','KK游戏直播',
        '阿麦直播','全球电视台网络直播湖南卫视网上直播','微吼直播','夜夜直播','视频比赛直播应用','九秀美女直播',
        'NBA直播','我秀娱乐直播','么么直播美女视频','51美女直播间','白兔美女直播','KK直播','微播美女直播',
        'CC直播','聚范直播','夜夜直播','AU直播','直播代购','小爱直播-美女直播','栗子直播','压寨直播',
        '哈尼直播','触手直播','明星直播','NOW直播','大王直播','星瞳直播','XingYueVRZhiBo','斗鱼直播',
        '风云直播','触手直播','全民直播','映客直播','NOW直播','CCTVNews','QiEDianJing','QiXiuZhiBo',
        'QiETV','LaoYouZhiBo','KuGouZhiBo','九秀美女直播','NBA直播','我秀娱乐直播','么么直播美女视频',
        '51美女直播间','白兔美女直播','KK直播','微播美女直播','CC直播','聚范直播','AU直播','小爱直播-美女直播',
        '栗子直播','压寨直播','哈尼直播','触手直播','明星直播'
        ) then '直播'
        else '其他视频' end as app_video_type
        from datamart.data_dw_xdr_gprs_app
        where app_type = '视频' or app_type ='视频直播'
        union all
        select app_id, app_name
        ,case when app_name in 
        (
        '咪咕音乐','QQ音乐','酷我音乐','千千静听','百度音乐','一听音乐','九酷音乐','DJ音乐盒','搜狗音乐','虾米音乐',
        '多米音乐','谷歌音乐盒','A67手机音乐','九天音乐','沃音乐','中国电信音乐门户','豆瓣FM','凤凰电台','酷狗音乐',
        '懒人听书','美乐电台','阿里星球','音悦台','巨鲸音乐','百宝','OVI音乐','中央音乐平台全曲下载平台','开心听',
        '爱音乐','全曲下载','网易云音乐','蜻蜓FM','喜马拉雅听','荔枝FM','多听FM','考拉FM电台','龙卷风收音机',
        '猎曲奇兵','酷狗FM','移动练歌房','善听听书','凤凰FM','企鹅fm','猜歌王','Jango','MySpace','Vevo','MusiXMatch',
        'Sing Karaoke','EnergyRadio','SongTaste音乐分享网','Rhapsody','Shazam','YandexMusic','NhacCuaTui音乐',
        'Zing mp3音乐','潮汐音乐','谷歌音乐','JooxMusic','Smule','MixerBox','MyWorld','JukeMusik','爱听','叮咚音乐',
        '听三零音乐网','DJ要听舞曲网','Echo回声音乐','Parsely','5ND','MC小洲','麦客疯','61宝宝网','61儿童网',
        '5Sing网','Mixcloud','Bandcamp','欢歌','新地DJ音乐网','Bugs','Mnet','Musicway','Saavn','雪峰音乐','清风DJ音乐网',
        'Xuefengmusic','捌零音乐论坛','DJ舞曲音乐网','djcc舞曲网','DJ前卫音乐','YYMP3','DJ总站','Mvbox','DJ舞曲','音乐网站',
        'Appdsp','Image Line官网','Alilo','Kiwi6','Netnaija','Jamendo','微米电台','Jing','音乐雷达','好多铃声','铃声梦工厂',
        '胎教音乐盒子','TXT听书','优听Radio','听听FM','阿基米德FM','宝贝听听','贝瓦儿歌','酷我音乐HD','musixmatch','voa慢速英语',
        '爱听4G','宝宝音乐会','DJ多多','DJ猫','华为音乐','中国好铃声','被窝音乐','迷上铃声','伤感电台','钢琴谱大全','听世界听书',
        '氧气听书','酷听听书','每日必听英语','Spotify','Vkontakte','真心点歌','听闻','5Sing','爱听360听书','口袋故事听听','天行听书',
        'A8音乐网','酷我音乐盒','DJ音乐厅','DEEZER','美国之音','音乐猎手','FM收音机','晚安电台','儿童故事电台','清风DJ无效版','咪咕铃声',
        '叮咚音乐','炫音社','音乐播放器','经典儿歌动画版','社交音乐平台','音乐分享','幻音二次元音乐','啪啪音乐圈','3D环绕音乐','12530',
        '中国移动音乐门户','流行的鈴聲','pandora','落网','余音','最全钢琴谱','鼎梵音乐','中兴音乐','轻听','ik6','果酱音乐','DJDuoDuo',
        'RightPaddle','LeQuMusic','KuGouLingSheng','咪咕音乐','虾米音乐','多米音乐','QQ音乐','酷狗音乐','懒人听书','天天动听','音悦台',
        '中央音乐平台全曲下载平台','酷我音乐盒','微米电台','开心听','九天音乐','一听音乐','搜狗音乐','九酷音乐','DJ音乐厅','联通音乐',
        '美乐电台','巨鲸音乐','音乐雷达','好多铃声','爱音乐','铃声梦工厂','TXT听书','音乐随身听','优听Radio','听听FM','阿基米德FM',
        '宝贝听听','贝瓦儿歌','酷我音乐HD','musixmatch','voa慢速英语','爱听4G','DJ多多','DJ猫','华为音乐','中国好铃声','全曲下载',
        '被窝音乐','迷上铃声','伤感电台','钢琴谱大全','听世界听书','氧气听书','酷听听书','每日必听英语','Spotify','豆瓣FM','Vkontakte',
        '真心点歌','听闻','5Sing','网易云音乐','爱听360听书','口袋故事听听','天行听书','A8音乐网','DEEZER',
        'OVI音乐','蜻蜓FM','音乐猎手','FM收音机','儿童故事电台','清风DJ无效版','酷我音乐','咪咕铃声','叮咚音乐','喜马拉雅听','炫音社',
        '音乐播放器','社交音乐平台','音乐分享','荔枝FM','幻音二次元音乐','啪啪音乐圈','12530','中国移动音乐门户',
        '多听FM','流行的鈴聲','pandora','落网','余音','最全钢琴谱','考拉FM电台','龙卷风收音机','猎曲奇兵','酷狗FM','移动练歌房','善听听书',
        '凤凰FM','百度音乐','网易云音乐'
        ) then '可听音乐'
        when app_name in 
        (
        'K歌达人','唱吧','爱唱','全民K歌','天籁K歌','KK唱响','麦唱','大家来k歌','恶搞变声器(WeChat Vocie)','爱唱K','八度K歌',
        '爱吼K歌','天天唱','K米','秒唱','教你学唱歌','神奇变声器','我要写歌','爱玩吉他','KuGooKTV','K歌达人','KK唱响','麦唱',
        '恶搞变声器(WeChat Vocie)','唱吧','全民K歌','爱唱K','天籁K歌','爱吼K歌','K米','秒唱','教你学唱歌','爱唱',
        ) then '可唱音乐'
        else '其他音乐' end as app_video_type
         where  app_type='音乐' 
 )

--
--创建表用户8月音视频流量分类统计     
drop table workspace.lkl_video_app_user_201908_d;
use workspace;
create table workspace.lkl_video_app_user_201908_d
    row format delimited fields terminated by '\001' 
    stored as orc tblproperties ('orc.compress'='ZLIB') 
as
select a.phone_no, a.app_video_type, round(sum(a.flow)/1024,2) as flow
from
(
    select a.phone_no, a.app_id, a.flow, b.app_video_type
    from
    (
        select phone_no, app_id, flow 
        from datamart.data_dw_xdr_gprs
        where dy = 2019 and dm = 08
    ) a
    inner join workspace.lkl_video_app_fenlei_201908_d b 
        on b.app_id = a.app_id
) a
group by a.phone_no, a.app_video_type;


--导入文本文件
load data local inpath '/mnt/disk1/user/lkl/input' into table 

-----分类目标用户音视频分类方式lkl_target_user_ring_video_type_201908_a
drop table workspace.lkl_target_user_ring_video_type_201908_a;
use workspace;
create table workspace.lkl_target_user_ring_video_type_201908_a
    row format delimited fields terminated by '\001' 
    stored as orc tblproperties ('orc.compress'='ZLIB') 
    as 
    select a.phone_no,longvideo,longvideolevel,shortvideo,shortvideolevel,livevideo,livevideolevel,othervideo,othervideolevel
    ,songmusic,songmusiclevel,lismusic,lismusiclevel    
    from 
    (
        select a.phone_no as phone_no,sum(a.longvideo) as longvideo,
        case when sum(a.longvideo)<10.57 and sum(a.longvideo)>0 then '轻度依赖'
        when sum(a.longvideo)>=10.57 and sum(longvideo) <260.51 then '中度依赖'
        when sum(a.longvideo)>=260.51 then '重度度依赖'
        else 'null' end as  longvideolevel,sum(a.shortvideo) as shortvideo,
        case when  sum(a.shortvideo)>0 and sum(a.shortvideo)<30.27 then '轻度依赖'
        when sum(a.shortvideo)>=30.27 and sum(shortvideo) <1145.99 then '中度依赖'
        when sum(a.shortvideo)>=1145.99 then '重度度依赖'
        else 'null' end as shortvideolevel,sum(a.livevideo) as livevideo,
        case when sum(a.livevideo)>0 and sum(a.livevideo)<0.09 then '轻度依赖'
        when sum(a.livevideo)>=0.09 and sum(livevideo) <1.18 then '中度依赖'
        when sum(a.livevideo)>=1.18 then '重度度依赖'
        else 'null' end as livevideolevel,sum(a.othervideo) as othervideo,
        case when sum(a.othervideo) > 0 and sum(a.othervideo)<0.56 then '轻度依赖'
        when sum(a.othervideo)>=0.56 and sum(othervideo) <49.25 then '中度依赖'
        when sum(a.othervideo)>=49.25 then '重度度依赖'
        else 'null' end as othervideolevel,sum(a.songmusic) as songmusic,
        case when sum(a.songmusic) > 0 and  sum(a.songmusic)<0.25 then '轻度依赖'
        when sum(a.songmusic)>=0.25 and sum(songmusic) <1.18 then '中度依赖'
        when sum(a.songmusic)>=1.18 then '重度度依赖'
        else 'null' end as songmusiclevel,sum(a.lismusic) as lismusic,
        case when sum(a.lismusic) > 0 and sum(a.lismusic)<2.37 then '轻度依赖'
        when sum(a.lismusic)>=2.37 and sum(lismusic) <7.43 then '中度依赖'
        when sum(a.lismusic)>=7.43 then '重度度依赖'
        else 'null' end as lismusiclevel
        from 
        (
            select a.phone_no,
            case when  a.app_video_type='长视频' then a.flow else 0 end as longvideo,
            case when a.app_video_type='短视频' then a.flow else 0 end as shortvideo,
            case when  a.app_video_type='直播' then a.flow else 0 end as livevideo,
            case when  a.app_video_type='其他视频' then a.flow else  0 end as othervideo,
            case when  a.app_video_type='可唱音乐' then a.flow  else 0 end as songmusic,
            case when  a.app_video_type='可听音乐' then a.flow else 0 end as lismusic
            from workspace.lkl_video_app_user_201908_d a
            order by a.phone_no 
        ) a
        group by a.phone_no
    ) a
    inner join 
    (
        select distinct a1.phone_no
        from
        (
            select a1.phone_no, count(a1.age) as count_no
            from workspace.hp_ring_term_opposite_no_user_20190826 a1 where a1.call_times >= 6
            group by a1.phone_no
    
        ) a1
        where a1.count_no > 10
    ) b
        on a.phone_no=b.phone_no
        

--分类目标用户音视频偏好分类lkl_target_user_ring_video_type_201908_d
drop table workspace.lkl_target_user_ring_video_type_201908_d;
use workspace;
create table workspace.lkl_target_user_ring_video_type_201908_d
    row format delimited fields terminated by '\001' 
    stored as orc tblproperties ('orc.compress'='ZLIB') 
as
 
 select b.phone_no,a.app_video_type,a.flow,a.usertype 
 from (
    select a.phone_no,a.app_video_type,a.flow,
    case   when a.flow<30.27 then  '短视频轻度依赖'
    when  (a.flow>=30.27 and a.flow <1145.99) then  '短视频中度依赖'
    when flow>=1145.99 then  '短视频重度依赖'
    else 'null' end as usertype 
    from lkl_video_app_user_201908_d a
    where a.app_video_type='短视频'
    union all
    select a.phone_no,a.app_video_type,a.flow,
    case  when a.flow<10.57 then  '长视频轻度依赖'
    when   a.flow>=10.57 and a.flow <260.51 then  '长视频中度依赖'
    when  a.flow>=260.51 then  '长视频重度度依赖'
    else 'null' end as usertype 
    from lkl_video_app_user_201908_d a
    where a.app_video_type='长视频'
    union all
    select a.phone_no,a.app_video_type,a.flow,
    case  when a.flow<0.09 then  '直播轻度依赖'
    when a.flow>=0.09 and a.flow<1.18 then  '直播中度依赖'
    when a.flow>=1.18 then  '直播重度依赖'
    else 'null' end as usertype 
    from lkl_video_app_user_201908_d a
    where a.app_video_type='直播'
    union all
    select a.phone_no,a.app_video_type,a.flow,
    case  when  a.flow<0.56 then  '其他视频轻度依赖'
    when a.flow>=0.56 and a.flow<49.25 then  '其他频中度依赖'
    when a.flow>=49.25 then  '其他频重度依赖'
    else 'null' end as usertype 
    from lkl_video_app_user_201908_d a
    where a.app_video_type='其他视频'
    union all
    select a.phone_no,a.app_video_type,a.flow,
    case  when a.flow<2.37 then  '可听音乐轻度依赖'
    when a.flow>=2.37 and a.flow<84.73 then  '可听音乐中度依赖'
    when  a.flow>=84.73 then  '可听音乐重度依赖'
    else 'null' end as usertype 
    from lkl_video_app_user_201908_d a
    where a.app_video_type='可听音乐'
    union all
    select a.phone_no,a.app_video_type,a.flow,
    case  when a.flow<0.25 then  '可唱音乐轻度依赖'
    when  a.flow>=0.25 and a.flow<7.43 then  '可唱音乐中度依赖'
    when   a.flow>=7.43 then  '可唱音乐重度依赖'
    else 'null' end as usertype 
    from lkl_video_app_user_201908_d a
    where a.app_video_type='可唱音乐'
  ) a   
inner join 
    (
    select distinct a1.phone_no
    from
    (
        select a1.phone_no, count(a1.age) as count_no
        from workspace.hp_ring_term_opposite_no_user_20190826 a1 where a1.call_times >= 6
        group by a1.phone_no
    ) a1
        where a1.count_no > 10
    ) b
    on a.phone_no=b.phone_no;
    
--创建文本表投诉电话号码
 create table workspace.lkl_camplaint_phone_2019_08
(
    phone_no string comment '投诉电话'
  
) 
row format delimited fields terminated by ',' lines terminated by '\n' 
stored as textfile;
--导入文本文件
load data local inpath '/mnt/disk1/user/lkl/input/complaintphone.txt' into table workspace.lkl_camplaint_phone_2019_08;

---刘億需求

drop table workspace.lkl_camplaint_phone_details_201908;
use workspace;
create table workspace.lkl_camplaint_phone_details_201908
    row format delimited fields terminated by '\001' 
    stored as orc tblproperties ('orc.compress'='ZLIB') 
as
select a.phone_no,b.times,b.netin_times,b.cu_times,b.ct_times,c.aprfee,c.mayfee,c.junfee,c.julfee
from workspace.encrypt_lkl_camplaint_phone_2019_08  a
left join
(  
    select a.phone_no,sum(b.times) as times,sum(b.netin_times) as netin_times,sum(b.cu_times) as cu_times, sum(b.ct_times) as ct_times
    from workspace.encrypt_lkl_camplaint_phone_2019_08 a   
    left join 
    (
        select a.phone_no,a.times as times,a.netin_times as netin_times,a.cu_times as cu_times,a.ct_times as ct_times        
        from datamart.dw_user_call_cal a 
        where dy=2019 and dm=05
        union all
        select a.phone_no,a.times as times,a.netin_times as netin_times,a.cu_times as cu_times,a.ct_times as ct_times       
        from datamart.dw_user_call_cal a 
        where dy=2019 and dm=06
        union all
        select a.phone_no,a.times as times,a.netin_times as netin_times,a.cu_times as cu_times,a.ct_times as ct_times      
        from datamart.dw_user_call_cal a 
        where dy=2019 and dm=07
    ) b
    on a.phone_no =b.phone_no
    group by a.phone_no
    
    
)b
on a.phone_no =b.phone_no
left join
(
    select a.phone_no,a.fee as aprfee
    ,LEAD(a.fee,1,null) over(partition by a.phone_no order by a.rn) as mayfee
    ,LEAD(a.fee,2,null) over(partition by a.phone_no order by a.rn) as junfee
    ,LEAD(a.fee,3,null) over(partition by a.phone_no order by a.rn) as julfee
    
    from
    (
        select a.phone_no,a.fee,row_number() over(distribute by a.phone_no sort by dm desc) as rn
        from
        (
            select a.phone_no,a.fee,a.dy,a.dm      
            from datamart.dw_user_call_cal a 
            where dy=2019 and dm=04
            union all
            select a.phone_no,a.fee, a.dy,a.dm      
            from datamart.dw_user_call_cal a
            where dy=2019 and dm=05
            union all
            select a.phone_no,a.fee, a.dy,a.dm      
            from datamart.dw_user_call_cal a
            where dy=2019 and dm=06
            union all
            select a.phone_no,a.fee,a.dy,a.dm        
            from datamart.dw_user_call_cal a
            where dy=2019 and dm=07
        ) a
    ) a

) c
on a.phone_no =c.phone_no ;



