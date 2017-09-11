from PIL import Image
import numpy as np
import cv2
import pytesseract

src_old = 'D:/Setup/workingpython/test.bmp'

col = Image.open("D:/Setup/workingpython/traindata/data/result_4.bmp")
gray = col.convert('L')

# Let numpy do the heavy lifting for converting pixels to pure black or white
bw = np.asarray(gray).copy()

# Pixel range is 0...255, 256/2 = 128
bw[bw < 128] = 80    # Black
bw[bw >= 128] = 1 # White
bw[bw >= 70] = 255

  
# Now we put it back in Pillow/PIL land
imfile = Image.fromarray(bw)
imfile.save("D:/Setup/workingpython/traindata/data/result_5.bmp")

src_path = 'D:/Setup/workingpython/result_1.bmp'

img = cv2.imread(src_old)

cv2.imshow("test", img)

cv2.waitKey(0)           

cv2.destroyAllWindows()


print(pytesseract.image_to_string(Image.open(src_old), lang='vie'))


#
#avg_color_per_row = np.average(img, axis=0)
#avg_color = np.average(avg_color_per_row, axis=0)
#print(avg_color)
#
#
