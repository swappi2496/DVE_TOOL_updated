
import fitz
from typing import Tuple
import os

def convert_pdf2img(input_file: str, pages: Tuple = None):

    pdfIn = fitz.open(input_file)
    output_files = []
    # Iterate throughout the pages
    for pg in range(pdfIn.pageCount):
        if str(pages) != str(None):
            if str(pg) not in str(pages):
                continue
        # Select a page
        page = pdfIn[pg]
        rotate = int(0)
        zoom_x = 2
        zoom_y = 2
        mat = fitz.Matrix(zoom_x, zoom_y).preRotate(rotate)
        pix = page.getPixmap(matrix=mat, alpha=False)
        output_file = f"{os.path.splitext(os.path.basename(input_file))[0]}_page{pg+1}.png"
        pix.writePNG(output_file)
        output_files.append(output_file)
    pdfIn.close()
    summary = {
        "File": input_file, "Pages": str(pages), "Output File(s)": str(output_files)
    }
    # Printing Summary
    print("## Summary ########################################################")
    print("\n".join("{}:{}".format(i, j) for i, j in summary.items()))
    return output_files

