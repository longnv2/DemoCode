import cv2
import numpy as np
from PIL import Image
import pytesseract
src_path = "D:/Setup/workingpython/traindata/data/"
#black_image = 'black_thesv.png'
binary = 'binary.png'
new_image = 'new_binary.png'

# load image
img = cv2.imread(src_path + binary) 
rsz_img = cv2.resize(img, None, fx=0.25, fy=0.25) # resize since image is huge
gray = cv2.cvtColor(rsz_img, cv2.COLOR_BGR2GRAY) # convert to grayscale


retval, thresh_gray = cv2.threshold(gray, thresh=100, maxval=255, type=cv2.THRESH_BINARY)


points = np.argwhere(thresh_gray==0) 
points = np.fliplr(points)
x, y, w, h = cv2.boundingRect(points)
x, y, w, h = x-10, y-10, w+20, h+20 
crop = gray[y:y+h, x:x+w] 

# get the thresholded crop
retval, thresh_crop = cv2.threshold(crop, thresh=200, maxval=255, type=cv2.THRESH_BINARY)
print(thresh_crop)
imfile  = Image.fromarray(thresh_crop)
imfile.save(src_path + new_image)

# display
cv2.imshow("Cropped and thresholded image", thresh_crop) 
cv2.waitKey(0)

print (pytesseract.image_to_string(Image.open(src_path + new_image), lang='vie'))