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
old_image = 'original/image_15.jpg'
#black_image = 'black_thesv.png'
binary = 'binary/binary.png'



# convert image to binary and crop 
def crop_image_from_binary(original, binary):
    # read image
    img = cv2.imread(original, 0) 
    
    # convert image to binary
    retval, thresh_gray = cv2.threshold(img, thresh=200, maxval=255, type=cv2.THRESH_BINARY)
    
    # crop image
    points = np.argwhere(thresh_gray==0) 
    points = np.fliplr(points)
    x, y, w, h = cv2.boundingRect(points)
    x, y, w, h = x, y-20, w, h+10
    crop = img[y:y+h, x:x+w] 
    
    retval, thresh_crop = cv2.threshold(crop, thresh=200, maxval=255, type=cv2.THRESH_BINARY)
    
    # save image
    imfile  = Image.fromarray(thresh_crop)
    imfile.save(binary)
    
crop_image_from_binary(src_path + old_image, src_path + binary)

#show_image(src_path + binary)

recognnize_text_from_image(src_path + binary)

# Recognize text from new image
def recognnize_text_from_image(path):
    # Read image from source
    # img = cv2.imread(path + black_image)
    print (pytesseract.image_to_string(Image.open(path), lang='vie+vie2+vie3+vie5'))







plt.imshow(Image.open(src_path + binary))

im = Image.new('RGBA', size, 'Black')
im.save(src_path + binary)



imp = Image.open(src_path + binary)

print(imp)




def show_image(src_path):
    image = cv2.imread(src_path)
    cv2.imshow('image', image)
    cv2.waitKey(0)
    cv2.destroyAllWindows()
    
show_image(src_path + new_binary)

show_image(src_path + binary)




#  Main funcion
if __name__ == '__main__':
    
#    convert_to_black_white(src_path + old_image, src_path + black_image)
    convert_to_white_black(src_path + old_image, src_path + binary)
#    recognnize_text_from_image(src_path + black_image)
    recognnize_text_from_image(src_path + new_binary)





#
#
## Convert picture to black $ white
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
#    bw[bw < 120] = 1    # Black
#    bw[bw > 120] = 255
##    bw[bw >= 128] = 255 # White
##    bw[bw >= 84] = 1
#    
#    # Save new Image      
#    imfile = Image.fromarray(bw)
#    imfile.save(new_image)
#
#
##print(np.asarray(Image.open(src_path + binary)))
#
#convert_to_black_white(src_path + old_image, src_path + binary)
##show_image(src_path + binary)
#
## Convert picture to black $ white
#def convert_to_white_black(old_image, new_image):
##    load original image
#    img = cv2.imread(old_image, 0)
#    img = cv2.medianBlur(img, 5)
#    ret, th1 = cv2.threshold(img,127,255,cv2.THRESH_BINARY)
##    convert to binary
##    plt.imshow(th1, 'gray')
#    imfile = Image.fromarray(th1)
#    imfile.save(new_image)
#
#convert_to_white_black(src_path + old_image, src_path + new_binary)
#    


    
    