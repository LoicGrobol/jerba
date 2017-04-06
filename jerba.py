#!/usr/bin/env python3
# -*- coding: utf-8 -*-

####
# Copyright (C) 2016 Kim Gerdes
# kim AT gerdes. fr
#
# This program is free software; you can redistribute it and/or
# modify it under the terms of the GNU General Public License
# as published by the Free Software Foundation; either version 2
# of the License, or (at your option) any later version.
####

import glob
import shutil
import time
import os
import argparse
import subprocess
import math

import logging
# logging.basicConfig(level=logging.INFO)

import psutil
import marisa_trie

import regex as re


def parsing(infile, outfolder="parses/", memory=None, cores=None, lemmatized=False):
    """
    parsing function
    parserType is always graph!
    infile: an empty conll file

    """
    outfile = os.path.join(outfolder, os.path.basename(infile))
    if outfile.endswith(".empty.conll"):
        outfile = outfile[:-len(".empty.conll")]

    if memory is None:
        memory = str(math.floor(3*psutil.virtual_memory().available/4000000000))+"G"
    if cores is None:
        cores = str(psutil.cpu_count - 1)

    logging.info('Using %s of memory', memory)
    logging.info('Using %s cores', cores)

    anna, lemclass, tagclass, parseclass = "mate/anna-3.61.jar", "is2.lemmatizer.Lemmatizer", "is2.tag.Tagger", "is2.parser.Parser"

    modelFolder = "models/"
    lemodel = modelFolder+"LemModel"
    tagmodel = modelFolder+"TagModel"
    parsemodel = modelFolder+"ParseModel"

    lemcommand = "java -XX:+UseG1GC -Xmx{memory} -cp {anna} {lemclass} -cores {cores} -model {lemodel} -test {infile} -out {outfile}.lem.conll".format(memory=memory, anna=anna, lemclass=lemclass, infile=infile, lemodel=lemodel, outfile=outfile, cores=cores)
    tagcommand = "java -XX:+UseG1GC -Xmx{memory} -cp {anna} {tagclass} -cores {cores} -model {tagmodel} -test {outfile}.lem.conll -out {outfile}.tag.conll".format(memory=memory, anna=anna, tagclass=tagclass, tagmodel=tagmodel, outfile=outfile, cores=cores)
    parsecommand = "java -XX:+UseG1GC -Xmx{memory} -cp {anna} {parseclass} -cores {cores} -model {parsemodel} -test {outfile}.tag.conll -out {outfile}.parse.conll".format(memory=memory, anna=anna, parseclass=parseclass, parsemodel=parsemodel, outfile=outfile, cores=cores)

    if lemodel and lemodel[-1] != "/":
        logging.info("Lemmatizing")
        logging.debug(lemcommand)
        try:
            p1 = subprocess.run([lemcommand], shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE).check_returncode()
            logging.debug(str(p1.stdout).strip())
        except subprocess.CalledProcessError as e:
            logging.error('Failure while lemmatizing')
            logging.debug(str(p1.stderr).strip())
            raise e
    else:  # TODO: add this part (for non inflectional languages like Chinese and for pre-lemmatized files)
        if lemmatized:
            lemmafile = os.path.join(outfolder, os.path.basename(infile))
            logging.debug("copying {lemmafile} as lemma file".format(lemmafile=lemmafile))
            shutil.copyfile(lemmafile, lemmafile+".lem.conll")
        # else:
        #     print("adding toks as lems", outfolder+os.path.basename(infile))
        #     trees = conll.conllFile2trees(infile)
        #     with codecs.open(outfolder+os.path.basename(infile)+".lem.conll", "w", "utf-8") as lemf:
        #         for tree in trees:
        #             lemf.write(newconvert.treeToEmptyConll14Text(tree, lemma=False)+"\n")

    logging.info("Tagging")
    logging.debug(tagcommand)

    try:
        p1 = subprocess.run([tagcommand], shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        logging.debug(str(p1.stdout).strip())
    except subprocess.CalledProcessError as e:
        logging.error('Failure while tagging')
        logging.debug(str(p1.stderr).strip())
        raise e

    logging.info("Parsing")
    logging.debug(parsecommand)

    try:
        p1 = subprocess.run([parsecommand], shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        logging.debug(str(p1.stdout).strip())
    except subprocess.CalledProcessError as e:
        logging.error('Failure while parsing')
        logging.debug(str(p1.stderr).strip())
        raise e

    logging.info("Parsed")

    # if checkIntegrity(outfile+'_parse') == False:
    #     print "*********ERROR IN FILE", outfile+"_parse", "Please Try again*********"
    return outfile+".parse.conll"


def lireDictionnaires():
    """
    Lecture des fichiers du lexique
    """
    droporfeo = 'lexiqueMultiMots/'  # where we can find the lexique folder
    special_char_lst = []
    # onlyLetters = re.compile(ur'''^\w+$''', re.U+re.I)
    for lexfile in glob.glob(droporfeo+'*.sfplm'):
        # Lecture des fichiers .sfplm
        # print "reading", lexfile
        with open(lexfile, encoding="utf8") as f:
            for ligne in f:
                if len(ligne) and "\t" in ligne:
                    t, lem = ligne.strip().split("\t")
                    # if (not onlyLetters.match(t[0])) or (not onlyLetters.match(t[1:-1].replace("-", ""))) or not onlyLetters.match(t[-1]) :
                    special_char_lst.append(t)
    logging.info("%s special character words", len(special_char_lst))
    return marisa_trie.Trie(special_char_lst)


def removePuncsFromConllfile(conllfile):
    """
    transforms the roots (0) to empty (-1)
    """
    with open(conllfile, encoding="utf8") as f:
        conlltext = f.read()
        conlltext = conlltext.replace("0    _    punc", "-1    _    punc")
    with open(conllfile, "w", encoding="utf8") as f:
        f.write(conlltext)


def emptyFromSentence(sentencefile, special_words=None, outfolder="."):
    """
    sentencefile with one sentence per line --> conll14.empty.conll
    if remultimatch not empty, it is used for tokenization
    """
    newname = os.path.basename(sentencefile)
    if newname.endswith(".txt"):
        newname = newname[:-len(".txt")]
    newname += ".empty.conll"
    outname = os.path.join(outfolder, newname)
    with open(sentencefile, encoding="utf8") as f, open(outname, "w", encoding="utf8") as g:
        for i, li in enumerate(f):
            if not i % 1000:
                logging.info('Tokenized %s sentences', i)
            if special_words is not None:
                toks = list(tokenize(li, special_words))
            else:
                toks = simpletokenize(li)
            for num, tok in enumerate(toks):
                g.write("\t".join([str(num+1), tok]+["_"]*12)+'\n')
            g.write("\n")
    return outname


def simpletokenize(text, returnMatchInfo=False):
    """
    simple punctuation and space based tokenization

    specific for french (apostrophes are glued to the precedent word: d' un
    in english we'd need: do n't Mike 's
    """

    reponct = re.compile(r'''(\s*[.;:, !?\(\)§"'«»\d]+)''', re.U+re.M)  # prepare for default punctuation matching. removed - from list
    renogroupponct = re.compile(r'''(\s*[;:, «»\(\)"])''', re.U+re.M)  # signs that have to be alone - they cannot be grouped

    # do the remaining simple token-based splitting
    toks = []
    text = reponct.sub(r" \1 ", text).replace(" '", "'")  # spaces around punctuation, but not before hyphen (french specific!)
    text = renogroupponct.sub(r" \1 ", text).replace(" ~", "~")  # spaces around no group punctuation
    return text.split()


def numurltokenize(text, returnMatchInfo=False):
    """
    number and url tokenization

    """
    reurl = re.compile(r'''(https?://|\w+@)?[\w\d\%\.]*\w\w\.\w\w[\w\d~/\%\#]*(\?[\w\d~/\%\#]+)*''', re.U+re.M+re.I)
    resignswithnumbers = re.compile(r'''\d+[\d, .\s]+''', re.U+re.M)

    # do the url recognition
    toks = []  # couples : (tok, todo = 0, done = 1)
    laststart = 0
    for m in reurl.finditer(text):
        # print 'reurl:%02d-%02d: %s' % (m.start(), m.end(), m.group(0))
        toks += [(text[laststart:m.start()], 0), (m.group(0).strip(), 1)]
        # print toks
        laststart = m.end()
    toks += [(text[laststart:], 0)]
    # print toks

    # do the number recognition
    ntoks = []
    for text, done in toks:
        if done:
            ntoks += [(text, done)]
        else:
            laststart = 0
            for m in resignswithnumbers.finditer(text):
                # print 'resignswithnumbers:%02d-%02d: %s' % (m.start(), m.end(), m.group(0))
                ntoks += [(text[laststart:m.start()], 0), (m.group(0).strip(), 1)]
                # print toks
                laststart = m.end()
            ntoks += [(text[laststart:], 0)]
    # print ntoks

    if returnMatchInfo:
        return ntoks
    else:
        return [t for t, done in ntoks]


def tokenize(line, special_words):
    """
    tokenization of line
    uses remultimatch = compiled list of words ordered by size (bigger first)
    returns list of tokens
    """

    # prepare line:
    line = line.replace(u"’", "'")
    respaces = re.compile(r'\s+')
    line = respaces.sub(r" ", line)

    for chunk, done in numurltokenize(line, returnMatchInfo=True):
        if done:
            yield chunk
        else:
            last_start = 0
            current = 1
            while last_start < len(chunk) - 1:
                while any(special_words.iter_prefixes(chunk[last_start:current])) and current < len(chunk):
                    current += 1

                if chunk[last_start:current-1] in special_words:
                    yield chunk[last_start:current-1]
                    last_start = current
                else:
                    next_tok = simpletokenize(chunk[last_start:])[0]
                    yield next_tok
                    last_start += len(next_tok)

                # Advance to the next non-space character
                while chunk[last_start].is_space():
                    last_start += 1
                current = last_start + 1


def parseSentenceFile(sentence_file, special_words, out_folder=None, memory=None, cores=None, remove_punct=True):
    """
    main function
    sentencefile: file with one sentence per line
    remultimatch: compiled re to match multiwords
    """
    if out_folder is None:
        out_folder = "parses/"

    logging.info("Preparing %s…", sentence_file)
    empty_conll = emptyFromSentence(sentence_file,
                                    special_words=special_words, outfolder=out_folder)
    logging.info("Parsing %s…", empty_conll)
    parsed_file = parsing(empty_conll, outfolder=out_folder, memory=memory, cores=cores)
    logging.info("Cleaning %s…", parsed_file)
    if remove_punct:
        removePuncsFromConllfile(parsed_file)


if __name__ == "__main__":

    parser = argparse.ArgumentParser(description='wrapper of tokenizer and mate parser with French syntactic models')
    parser.add_argument('-s', '--sentence', help='sentence between quotes', required=False)
    parser.add_argument('-f', '--sentencesFile', help='file containing one sentence per line', required=False)
    parser.add_argument('-m', '--memory', help='amount of memory used (eg. `10G`)', default=None, required=False)
    parser.add_argument('-c', '--cores', help='number of cores to use', default=None, required=False)
    parser.add_argument('-v', '--verbose', action='store_true')

    args = vars(parser.parse_args())

    if args['verbose']:
        logging.basicConfig(level=logging.DEBUG)
    else:
        logging.basicConfig(level=logging.INFO)

    ti = time.time()

    logging.info('Reading dictionaries')
    special_words = lireDictionnaires()

    if args.get("sentencesFile", None):
        parseSentenceFile(args.get("sentencesFile", None), special_words=special_words,
                          memory=args['memory'], cores=args['cores'])
    if args.get("sentence", None):
        with open("parses/singleSentence.txt", "w", encoding="utf8") as singleSentenceFile:
            singleSentenceFile.write(args.get("sentence", None)+"\n")
            parseSentenceFile("parses/singleSentence.txt", special_words=special_words,
                              memory=args['memory'], cores=args['cores'])
        with open("parses/singleSentence.parse.conll", encoding="utf8") as singleSentenceParse:
            print(singleSentenceParse.read())

    logging.info("it took {duration} seconds".format(duration=time.time()-ti))
