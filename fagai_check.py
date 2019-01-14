# -*- coding: utf-8 -*-
"""
@author: MonsterHe
@contact: yuntian.hee@gmail.com
@version: python3.6
@file: open_sql.py
@time: 2018/11/3 10:26
@tools: Pycharm 2018.1
"""
"""
update:
优化了循环逻辑
简化了数据库路径配置
"""

"""
当前检查的功能：（*标记的为未实现）
1.非内容字段包含HTML标签  
2.内容标签截断
3.标题、发布时间格式错误
4.标签采集结果为空
6.非标讯检查
11.内容中包含input标签，未把标签内的值替换出来（在后续流程中标签会被去掉，这样会导致未提取的值也会被去掉造成内容不全
*5.采集非本站数据
*7.数据处理（空格替换为空）
*8.cookie不是用脚本获取的
*9.任务备注里面包含 ""或''
*10.内容是json格式或源页面json解析异常而未处理
"""

import sqlite3
import re


r_path = r'''D:\7个火车头\25发改修复\Configuration\config.db3'''
j_path = r'''D:\7个火车头\25发改修复\Data\%s\SpiderResult.db3'''

# 连接数据库
def connect_db(db_path): 
    return sqlite3.connect(db_path)

# 检查规则
def check_rule(siteid, db_path):

    #校验
    sqlrule_first = """
    SELECT t.jobid,t.jobname,s.siteid,s.sitename FROM "Job" t,site s
    where
    t.siteid = s.siteid
    and t.siteid  in (%s)
    limit 1
    """%siteid
    
    #检查历史规则任务名
    sqlhis_check = """
    SELECT t.jobid,t.jobname,s.siteid,s.sitename FROM "Job" t,site s
    where
    t.siteid = s.siteid
    and t.siteid  in (%s)
    and s.sitename like '%%%%历史%%%%'
    and t.jobname not like '%%%%历史%%%%'
    """%siteid

    #检查任务名长度；仅供参考
    sqljobnamelength = """
    SELECT t.jobid,t.jobname,s.siteid,s.sitename FROM "Job" t,site s
    where
    t.siteid = s.siteid
    and t.siteid  in (%s)
    and length(t.JobName) > 20
    """%siteid
    
    #检查发布时间为系统时间是否标记-A
    sqldatecheck ="""
    SELECT jobid,jobname FROM "Job" t
    where t.siteid  in (%s)
    and (instr(t.xmldata,'ManualTimeStr="yyyy-MM-dd"')
    and JobName NOT LIKE '%%%%-A%%%%'
    )
    """%siteid

    #检查使用系统时间做历史的规则
    sqldatecheckhis ="""
    SELECT jobid,jobname FROM "Job" t
    where t.siteid  in (%s)
    and (instr(t.xmldata,'ManualTimeStr="yyyy-MM-dd"')
    and JobName  LIKE '%%%%历史%%%%'
    )
    """%siteid
    
    #标讯标签
    sql1 = """
    SELECT t.jobid,t.jobname,s.siteid,s.sitename FROM "Job" t,site s
    where
    t.siteid = s.siteid
    and t.siteid  in (%s)
    and (instr(t.xmldata,'<Rule LabelName="发布时间"') =0
    or instr(t.xmldata,'<Rule LabelName="内容"') =0
    or instr(t.xmldata,'<Rule LabelName="来源"') =0
    or instr(t.xmldata,'<Rule LabelName="公告类型"') =0
    or instr(t.xmldata,'<Rule LabelName="地区"') =0
    or instr(t.xmldata,'<Rule LabelName="采购编号"') =0
    or instr(t.xmldata,'<Rule LabelName="数据类型"') =0
    or instr(t.xmldata,'<Rule LabelName="标题"') =0
    );
    """%siteid

    sql2 = """
    SELECT jobid,jobname FROM "Job" t
    where t.siteid  in (%s)
    and instr(xmldata,'Rule LabelName="公告类型" GetDataType="0" StartStr="" EndStr="" RegexContent="" RegexCombine="" XpathContent="" XPathAttribue="0" TextRecognitionType="0" TextRecognitionCodeReturnType="1" LengthFiltOpertar="0" LengthFiltValue="0" LabelContentMust="" LabelContentForbid="" FileUrlMust="" FileSaveDir="" FileSaveFormat="" ManualType="0" ManualString="type01') =0
    and instr(xmldata,'Rule LabelName="公告类型" GetDataType="0" StartStr="" EndStr="" RegexContent="" RegexCombine="" XpathContent="" XPathAttribue="0" TextRecognitionType="0" TextRecognitionCodeReturnType="1" LengthFiltOpertar="0" LengthFiltValue="0" LabelContentMust="" LabelContentForbid="" FileUrlMust="" FileSaveDir="" FileSaveFormat="" ManualType="0" ManualString="type02') =0
    """%siteid
    sql3 = """
    SELECT jobid,jobname FROM "Job" t
    where t.siteid  in (%s)
    and instr(XmlData,'MaxSpiderPerNum="0" MaxOutPerNum="0"') = 0
    """%siteid
    sql4 = """
    SELECT jobid,jobname FROM "Job" t
    where t.siteid  in (%s)
    and instr(t.XmlData,'CheckUrlRepeat=""') = 0
    """%siteid
    sql5 = """
    SELECT jobid,jobname FROM "Job" t
    where t.siteid  in (%s)
    and(instr(t.XmlData,'new="201') > 0
    or instr(t.XmlData,'<FillBothEnd Start="201') > 0)
    """%siteid
    sql6 = """
    SELECT jobid,jobname FROM "Job" t
    where t.siteid  in (%s)
    and( instr(xmldata,'UrlRepeat=""') =0
    or instr(xmldata,'CheckUrlRepeat=""') =0)
    """%siteid
    sql7 = """
    SELECT JobId,JobName,substr(t.XmlData,instr(t.XmlData,'<Rule LabelName="数据类型')+345,3),substr(substr(XmlData,instr(XmlData,'<Rule LabelName="地区"'),420),instr(substr(XmlData,instr(XmlData,'<Rule LabelName="地区"'),420),'ManualString="')+13,9
    ),substr(substr(XmlData,instr(XmlData,'<Rule LabelName="来源"'),420),instr(substr(XmlData,instr(XmlData,'<Rule LabelName="来源"'),420),'ManualString="')+14,instr(substr(substr(XmlData,instr(XmlData,'<Rule LabelName="来源"'),420),instr(substr(XmlData,instr(XmlData,'<Rule LabelName="来源"'),420),'ManualString="')+13,40),'ManualTimeStr=')-4)
    FROM "Job" t
    where SiteId in (%s)
    """%siteid
    
    conn = connect_db(db_path)
    cursor = conn.cursor()
    try:
        row11 = cursor.execute(sqlrule_first)
        for x11 in row11:
            print("-------校验\n---------",x11,"\n------------")
        row1 = cursor.execute(sql1)
        for x1 in row1:
            print(x1, '缺少标签')
        row2 = cursor.execute(sql2)
        for x2 in row2:
            print(x2, '公告类型写死')
        row3 = cursor.execute(sql3)
        for x3 in row3:
            print(x3, '误设最大页数')
        row4 = cursor.execute(sql4)
        for x4 in row4:
            print(x4, '检查重复没有勾上')
        row5 = cursor.execute(sql5)
        for x5 in row5:
            print(x5, '规则里面有前缀后缀')
        row6 = cursor.execute(sql6)
        for x6 in row6:
            print(x6, '检查网址重复未选上')
        row7 = cursor.execute(sqldatecheck)
        for xdate in row7:
            print(xdate,'系统时间未标记-A')
        row71 = cursor.execute(sqldatecheckhis)
        for xdate in row71:
            print(xdate,'系统时间不能做历史')
        row72 = cursor.execute(sqljobnamelength)
        for xdate in row72:
            print(xdate,'任务名长度超过限定长度')
        row73 = cursor.execute(sqlhis_check)
        for xdate in row73:
            print(xdate,'历史任务名格式错误')
        print('\n --- check rule finish')
    except NotImplementedError as error:
        print(str(error))
    finally:
        cursor.close()
        conn.close()

# 环评规则检查
def env_check(siteid, db_path):
    
    #校验
    sqlrule_first = """
    SELECT t.jobid,t.jobname,s.siteid,s.sitename FROM "Job" t,site s
    where
    t.siteid = s.siteid
    and t.siteid  in (%s)
    limit 1
    """%siteid

    sqljobnamelength = """
    SELECT t.jobid,t.jobname FROM "Job" t
    where t.siteid  in (%s)
    and length(t.JobName) > 20
    """%siteid
    
    sqldatecheck ="""
    SELECT jobid,jobname FROM "Job" t
    where t.siteid  in (%s)
    and (instr(t.xmldata,'ManualTimeStr="yyyy-MM-dd"')
    and JobName NOT LIKE '%%%%-A%%%%'
    )
    """%siteid

    sqldatecheckhis ="""
    SELECT jobid,jobname FROM "Job" t
    where t.siteid  in (%s)
    and (instr(t.xmldata,'ManualTimeStr="yyyy-MM-dd"')
    and JobName  LIKE '%%%%历史%%%%'
    )
    """%siteid
    

    sql1 = """
    SELECT jobid,jobname FROM "Job" t
    where t.siteid  in (%s)
    and (instr(t.xmldata,'<Rule LabelName="发布时间"') =0
    or instr(t.xmldata,'<Rule LabelName="内容"') =0
    or instr(t.xmldata,'<Rule LabelName="来源"') =0
    or instr(t.xmldata,'<Rule LabelName="公告类型"') =0
    or instr(t.xmldata,'<Rule LabelName="地区"') =0
    or instr(t.xmldata,'<Rule LabelName="标题"') =0
    
    );
    """%siteid
    sql3 = """
    SELECT jobid,jobname FROM "Job" t
    where t.siteid  in (%s)
    and instr(XmlData,'MaxSpiderPerNum="0" MaxOutPerNum="0"') = 0
    """%siteid
    sql4 = """
    SELECT jobid,jobname FROM "Job" t
    where t.siteid  in (%s)
    and instr(t.XmlData,'CheckUrlRepeat=""') = 0
    """%siteid
    sql5 = """
    SELECT jobid,jobname FROM "Job" t
    where t.siteid  in (%s)
    and(instr(t.XmlData,'new="201') > 0
    or instr(t.XmlData,'<FillBothEnd Start="201') > 0)
    """%siteid
    sql6 = """
    SELECT jobid,jobname FROM "Job" t
    where t.siteid  in (%s)
    and( instr(xmldata,'UrlRepeat=""') =0
    or instr(xmldata,'CheckUrlRepeat=""') =0)
    """%siteid
    
    conn = connect_db(db_path)
    cursor = conn.cursor()
    
    try:
        row11 = cursor.execute(sqlrule_first)
        for x11 in row11:
            print("-------校验\n---------",x11,"\n------------")
        row11 = cursor.execute(sqlrule_first)
        row1 = cursor.execute(sql1)
        for x1 in row1:
            print(x1, '缺少标签')
        row3 = cursor.execute(sql3)
        for x3 in row3:
            print(x3, '误设最大页数')
        row4 = cursor.execute(sql4)
        for x4 in row4:
            print(x4, '检查重复没有勾上')
        row5 = cursor.execute(sql5)
        for x5 in row5:
            print(x5, '规则里面有前缀后缀')
        row6 = cursor.execute(sql6)
        for x6 in row6:
            print(x6, '检查网址重复未选上')
        row7 = cursor.execute(sqldatecheck)
        for xdate in row7:
            print(xdate,'系统时间未标记-A')
        row71 = cursor.execute(sqldatecheckhis)
        for xdate in row71:
            print(xdate,'系统时间不能做历史')
        row72 = cursor.execute(sqljobnamelength)
        for xdate in row72:
            print(xdate,'任务名长度超过限定长度')
        
        print('finish')
    except NotImplementedError as error:
        print(str(error))
    finally:
        cursor.close()
        conn.close()
# 检查内容
def check_content(job_path):
    # 检查非内容字段有无截断HTML标签
    sql1 = '''SELECT ID, 标题, 发布时间 FROM content
    WHERE 已采 = 1
    AND (标题 LIKE '%<%'
    OR 标题 LIKE '%>%'
    OR 发布时间 LIKE '%<%'
    OR 发布时间 LIKE '%>%')
    '''
    
    # 检查有空内容的字段
    sql2 = '''SELECT ID, 标题, 内容, 发布时间 FROM Content WHERE 已采=1
    AND (标题 IS NULL
    OR 内容 IS NULL
    OR 发布时间 IS NULL
    OR 内容=''
    OR 发布时间=''
    OR 标题='')
    '''

    # 检查内容标签中包含有input标签
    sql3 = '''SELECT ID, 标题 FROM Content WHERE 已采=1
    AND 内容 LIKE '%<input%'
     '''
    # 检查内容中有截断HTML标签
    sql4 = '''SELECT ID, 标题, 内容 FROM Content WHERE 已采=1
    '''
    # 检查采集结果是否不为标讯
    sql5 = '''SELECT ID, 标题, 内容 FROM Content WHERE 已采=1
    AND 标题 IS NOT NULL
    AND 标题 != ''
    AND 内容 IS NOT NULL
    AND 内容 != ''
    '''

    # 检查标题或发布时间格式是否正确
    sql6 = '''SELECT ID, 标题, 发布时间 FROM Content WHERE 已采=1
    AND 标题 IS NOT NULL
    AND 标题 != ''
    AND 发布时间 IS NOT NULL
    AND 发布时间 != ''
    '''


    conn = connect_db(job_path)
    cursor = conn.cursor()
    try:
        print('---------开始检查非内容字段是否有HTML标签--------')
        contents1 = cursor.execute(sql1)
        for job1 in contents1:
            print('{}\t {}\t标题或时间有HTML标签'.format(job1[0], job1[1]))

        print('\n---------开始检查内容、标题、发布时间是否有空--------')
        contents2 = cursor.execute(sql2)
        for job2 in contents2:
            print('{}\t {}\t内容或标题或发布时间有空值'.format(job2[0], job2[1]))

        print('\n---------开始检查内容字段是否有input标签--------')
        contents3 = cursor.execute(sql3)
        for job3 in contents3:
            print('{}\t {}\t内容有input标签没有过滤'.format(job3[0], job3[1]))

        print('\n---------开始检查内容字段是否有截断标签（开头处）--------')
        # 检查内容是否有截断HTML标签（目前只能检查开头）
        contents4 = cursor.execute(sql4)
        for job4 in contents4:
            content = job4[2]
            if isinstance(content, str):
                result = re.match(r"^.*?>", content)
                if result != None:
                    if result.group(0).count('<') != result.group(0).count('>'):
                        print("{}\t {}\t内容可能有截断标签".format(job4[0], job4[1]))
                    else:pass
                else:pass
            else:pass
        '''
        print('\n---------开始检查采集内容是否不为标讯（仅做参考）--------')
        contents5 = cursor.execute(sql5)
        for job5 in contents5:
            title = job5[1]
            content = job5[2]
            judge_value = check_title(title)
            # 标题若无法判断是否为标讯，则进入内容判断
            if judge_value != 1:
                judge_value = check_bidding(content)
                if judge_value != 1:
                    print('{}\t {}\t {tip}'.format(job5[0], job5[1], tip='采集内容不是标讯(经作参考)'))
            else:pass
        '''

        print('\n---------检查标题、发布时间格式是否正确--------')
        contents6 = cursor.execute(sql6)
        for job6 in contents6:
            title = job6[1]
            publishtime = job6[2]
            judge_value1 = check_title_form(title)
            if judge_value1 == -1:
                print('{}\t {}\t {tip}'.format(job6[0], job6[1], tip='标题前或后有空格'))
            judge_value2 = check_publishtime_form(publishtime)
            if judge_value2 == -1:
                print('{}\t {}\t {tip}'.format(job6[0], job6[1], tip='发布时间有空格'))
            if judge_value2 == -2:
                print('{}\t {}\t {tip}'.format(job6[0], job6[1], tip='发布时间格式错误'))
       
    except sqlite3.OperationalError as e:
        pass
    except NotImplementedError as e:
        print(str(e))
    finally:
        print('\n--- finish\n')
        cursor.close()
        conn.close()

# 检查非标讯
def check_title(s_title):
    targets = ['中标','招标','议标','废标','流标','邀标','询价','谈判','采购',
              '比选','磋商','成交','竞价','竞投','中选','邀请招标','投标',
              '竞谈','单一来源','竞争性','竞选','评标','项目结果公示','竞标',
              '摇珠','开标','招商','确标','询比价','补遗','中选','发包','补遗',
              '答疑','项目限价','竞标','竞选','议价','竟标','招租','询标','遴选','标段','工程'
               ]

    not_targets = ['条例','规定','工作','规范','标准','荣获','招聘','试行','行动']

    for target in targets:
        if target in s_title:
            return 1
        else:pass
    return -1

# 检查标讯是否被过滤
def check_bidding(s_content):
    targets = ['招标条件', '项目名称', '招标范围', '投标人资格', '投标人', '招标文件', '投标',
               '招标人', '标段名称', '中标人', '评标结果', '投标', '采购人', '单一来源编号', '招标编号',
               '采购组织', '标项名称', '采购项目', '采购方式', '成交结果', '原采购公告', '采购公告',
               '采购内容', '合格供应商', '采购文件', '保证金', '开标时间', '询价项目编号', '询价供应商',
               '标项名称', ]
    for target in targets:
        if target in s_content:
            return 1
        else:pass
    return None

# 检查标题格式
def check_title_form(s_content):
    result = re.search(r'''^\s*|\s*$''', s_content)
    if result.group() != '':
        return -1
    else:
        return None

# 检查发布时间格式
def check_publishtime_form(s_content):
    # 首尾空格检查
    check_block = re.search(r'''^\s*|\s*$''', s_content)
    if check_block.group() != '':
        return -1
    else:
        # 格式检查
        check_form = re.match(r'''^\d{4}-\d{1,2}-\d{1,2}$''', s_content)
        if check_form == None:
            return -2
        else:
            return None

def get_jobs(siteid, db_path):
    '''获取一个组的任务字典'''
    job_dic = {}
    sql = '''SELECT j.jobid, j.jobname FROM "Job"j WHERE j.siteid = %s order by j.jobid''' % siteid
    conn = connect_db(db_path)
    cursor = conn.cursor()
    try:
        jobs = cursor.execute(sql)
        job_dic = {jobid: jobname for (jobid, jobname) in jobs}
        return job_dic
    except sqlite3.OperationalError as e:
        pass
    except NotImplementedError as e:
        print(str(e))
    finally:
        cursor.close()
        conn.close()

def check_db():
    while True:
        try:
            siteid = int(input('input site ID = '))
            print('input flag:\n1 --- check rule \n2 --- check content \n3 --- all check \n')
            select_check = int(input('flag = '))
        except ValueError:
            print('Invalid input parameters, please enter an integer')
        else:
            if select_check == 1:
                # 检查规则
                env_check(siteid=siteid, db_path=r_path)
                print('--- END')
            elif select_check == 2:
                # 选择检查某一个任务，或全部任务
                job_dic = get_jobs(siteid=siteid,db_path=r_path)
                while True:
                    print('id ', '---', 'jobname')
                    for jobid, jobname in job_dic.items():
                        print(jobid, '---', jobname)
                    print('\ninput a flag:\n1 --- check all \n0 --- exit \nid --- select one job\n')
                    selectid = int(input('flag = '))
                    if selectid == 1:
                        for job_id, job_name in job_dic.items():
                            print("\n--- 开始执行检查：", job_name, job_id, "\n")
                            job_path = j_path %job_id
                            check_content(job_path)
                    elif selectid == 0:
                        print('--- END')
                        break
                    else:
                        job_path = j_path %selectid
                        check_content(job_path)

            else:
                print('--- exit')
                break

        

def main():
    check_db()

if __name__ == '__main__':
    main()



    

        





