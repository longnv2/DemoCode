from PIL import Image
import pytesseract
import cv2
import numpy as np
import pytesseract
import pylab as pl
import matplotlib.cm as cm
import matplotlib.pyplot as plt

#location for picture
src_path = "D:/data/"
old_image = 'original/image_12.jpg'
#black_image = 'black_thesv.png'
binary = 'binary/binary.png'
new_binary = 'crop_binary/crop_binary_1.png'

#def convert_to_black_white(old_image, new_image):
#    
#    # Convert old picture to Black & White
#    img = Image.open(old_image)
#    gray = img.convert('L')
#    
#    # Pixel range picture
#    bw = np.asarray(gray).copy()
#    
#    # Pixel range is 0...255, 256/2 = 128
##    bw[bw < 80] = 1    # Black
##    bw[bw >80] = 255
##    bw[bw >= 128] = 255 # White
##    bw[bw >= 84] = 1
#    
#    # Save new Image      
#    imfile = Image.fromarray(bw)
#    imfile.save(new_image)



    # read image
    img = cv2.imread(src_path + old_image) 
    
    # convert image to binary
    gray = cv2.cvtColor(img, cv2.COLOR_BGR2GRAY)
    retval, thresh_gray = cv2.threshold(gray, thresh=100, maxval=255, type=cv2.THRESH_BINARY)
    
    
    
    imfile  = Image.fromarray(thresh_gray)
    imfile.save(src_path + new_binary)
    
    
    # crop image
    points = np.argwhere(thresh_gray==0) 
    points = np.fliplr(points)
    x, y, w, h = cv2.boundingRect(points)
    x, y, w, h = x-10, y-10, w+20, h+20 
    crop = img[y:y+h, x:x+w] 
    
    
    retval, thresh_crop = cv2.threshold(crop, thresh=120, maxval=255, type=cv2.THRESH_BINARY)
    
    # save image
    imfile  = Image.fromarray(thresh_crop)
    imfile.save(src_path + new_binary)
    
    
    

