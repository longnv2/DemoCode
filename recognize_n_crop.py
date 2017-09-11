import cv2
import numpy as np
import pytesseract
from PIL import Image

# Path of working folder on Disk
img_path = "D:/data/original/image_15.jpg"
src_path = 'D:/data/original/'


# Read image with opencv
img = cv2.imread(img_path)

# Convert to gray
img = cv2.cvtColor(img, cv2.COLOR_BGR2GRAY)

# Apply dilation and erosion to remove some noise
kernel = np.ones((1, 1), np.uint8)
img = cv2.dilate(img, kernel, iterations=1)
img = cv2.erode(img, kernel, iterations=1)

# Write image after removed noise
cv2.imwrite(src_path + "removed_noise.png", img)

# Write the image after apply opencv to do some ...
cv2.imwrite(src_path + "thres.png", img)
#
img = Image.open(src_path + "thres.png")

#bw = np.asarray(img).copy()
#    
## Pixel range is 0...255, 256/2 = 128
#bw[bw < 120] = 1    # Black
#bw[bw > 120] = 255
#
#imfile = Image.fromarray(bw)
#imfile.save(src_path + "thres.png")

    # Recognize text with tesseract for python
print(pytesseract.image_to_string(Image.open(src_path + "thres.png"), lang='vie+vie2+vie3+vie5'))


