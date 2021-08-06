import zipfile
import os
import platform

system = platform.system()
print(system)


# 解压Zip包
def unzip_file(zip_src, dst_dir):
    r = zipfile.is_zipfile(zip_src)
    if r:
        fz = zipfile.ZipFile(zip_src, 'r')
        for file in fz.namelist():
            if system.startswith('L'):
                if not dst_dir.endswith('/'):
                    dst_dir += '/'
                # filename = package/0111b1f425b34d729c04d3b262c1061d/articleData.txt
                filepath = file.split('\\')
                dir = dst_dir + '/'.join(filepath[:-1])
                # print(filepath)
                if not os.path.exists(dir):
                    os.makedirs(dir)  # makedirs(path)
                fz.extract(file, dir)
                os.renames(dir + '/' + file, dir + '/' + filepath[-1])
            else:
                fz.extract(file, dst_dir)
        fz.close()
    else:
        print('This\'s not a zip file')

# unzip_file(r'C:\Users\hp\Desktop\2021-06-21 14_06_31_242dataInfo.zip', 'D:/tmp')
