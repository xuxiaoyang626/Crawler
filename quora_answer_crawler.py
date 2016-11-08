#!/usr/bin/env python
# -*- encoding: utf-8 -*-
# Created on 2016-11-03 14:32:37
# Project: quora
# Author: Xiaoyang Xu
'''
    This is a script using pyspider to crawl answers from quora. It reads a file of qustion links and grab the contnts
    of each question, including the question_title, question_body, answer_body, answer_time. The contents grabbed would
    be stored in MySQL databse locally. Each question would be stored as text while answers would be stored as html
    string(for further convinient usage).
'''
from pyspider.libs.base_handler import *
from bs4 import BeautifulSoup
import mysql.connector



class Handler(BaseHandler):

    crawl_config = {
    }
    
    # connect with the MySQL db
    def __init__(self):
        self.qid = 1
        self.db = mysql.connector.connect(
            user='root',
            password='root',
            host='localhost',
            database='quoradb'
        )
    
    # helper function to insert singe question into db
    def add_question(self, question_id, question_title, question_body):
        try:
            cursor = self.db.cursor()
            sql = 'insert into questions(question_id, question_title, question_body , post_time) values ("%d","%s","%s", %s)' % (question_id, question_title, question_body, 'now()');
            # print sql
            cursor.execute(sql)
            rowid = cursor.lastrowid
            self.db.commit()
            #print rowid
        except Exception, e:
            print e
            self.db.rollback()

    # helper function to insert singe single answer into db
    def add_answer(self, question_id, answer_body, answer_time):
        try:
            cursor = self.db.cursor()
            sql = 'insert into answers(question_id, answer_body, answer_time) values ("%d","%s","%s")' % (question_id, answer_body, answer_time);
            # print sql
            cursor.execute(sql)
            rowid = cursor.lastrowid
            self.db.commit()
            #print rowid
        except Exception, e:
            print e
            self.db.rollback()


    '''
        param: @every(minutes=24 * 60) run scrpits every 24 hours
        read a txt file contains question links locally and iterate through each link
    '''
    @every(minutes=24 * 60)
    def on_start(self):
        self.crawl('https://www.quora.com/What-are-the-admission-guidelines-at-CMU-MIT-and-Stanford-for-doing-an-MS-in-machine-learning', callback=self.index_page)
        '''
        file_question_urls = open('/Users/xuxiaoyang/Documents/CMU/15637-Web-Dev/teamproject/quora_crawl-master/Carnegie-Mellon-Universitysmall_question_urls.txt', mode = 'r')
        current_line = file_question_urls.readline()
        while (current_line):
            url = 'https://www.quora.com' + current_line
            self.crawl(url, callback=self.index_page)
            current_line = file_question_urls.readline()
        '''

    '''
        param: @config(age=10 * 24 * 60 * 60) links that have been visited within 10 days will be ignored
        entering each question's detail page and grab question detail and answer detail and store these info
        into db.
    '''
    @config(age=10 * 24 * 60 * 60)
    def index_page(self, response):
        # question detail
        question_title = response.doc('h1 span.rendered_qtext').text()
        question_body = response.doc('div.question_details').text()
        question_id = self.qid
        self.add_question(question_id, question_title, question_body)
        # answer detail
        html = response.doc('.AnswerPagedList').html()
        soup = BeautifulSoup(html)
        answer_htmls = soup.find_all("div", class_="pagedlist_item")
        for each in answer_htmls:
            if each.find("div", class_='logged_out_related_questions_heading'):
                # get rid of related side bar
                continue
            if each.find("div", class_='CollapsedAnswersSectionCollapsed'):
                # get rid of last useful pagelist item
                continue
            else:
                # remove footer of the answer
                each.find("div", class_='AnswerFooter').decompose()
                time = each.find("a", class_="answer_permalink").text.decode("utf8")
                answer = str(each).replace('"', '\\"').decode("utf8")
                question_id = self.qid
                self.add_answer(question_id , answer, time)
        self.qid = self.qid + 1
        # results have been inserted into MySQL locally. However, pyspider also has its own result management mechanism, the return dictionary will be stored as csv or JSON as well. 
        return { 
            'question': response.doc('h1 span.rendered_qtext').text(),
            'answers': response.doc('.AnswerPagedList').html()
        }